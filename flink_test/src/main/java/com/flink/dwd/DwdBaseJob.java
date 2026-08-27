package com.flink.dwd;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.DimUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * DWD 层单作业动态分流（借鉴尚硅谷 DwdBaseApp 业务逻辑，落 Doris 而非 Kafka）
 *
 * 核心：把 5 张表（credit_facility、credit_facility_status、reply、credit、contract）
 * 统一成 OdsRecord，union 成一条流，再分离 contract（走双流关联）和其余 4 表（走动态分流）。
 *
 * 动态分流逻辑封装在 {@link DwdProcessFunction}，输出统一为 JSON 字符串（String），
 * 规避 Flink 异构 POJO 侧输出类型传播导致的 ClassCastException。
 */
public class DwdBaseJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        // 读 5 张表，统一成 OdsRecord，union 成一条流
        DataStream<OdsRecord> creditStream = readTopic(env, "ods_credit", "dwd_base_credit");
        DataStream<OdsRecord> cfStream = readTopic(env, "ods_credit_facility", "dwd_base_cf");
        DataStream<OdsRecord> cfStatusStream = readTopic(env, "ods_credit_facility_status", "dwd_base_cf_status");
        DataStream<OdsRecord> replyStream = readTopic(env, "ods_reply", "dwd_base_reply");
        DataStream<OdsRecord> contractStream = readTopic(env, "ods_contract", "dwd_base_contract");

        DataStream<OdsRecord> allStream = creditStream
                .union(cfStream, cfStatusStream, replyStream, contractStream);

        // 分离 contract（走双流关联）和其他 4 张表（走动态分流）
        DataStream<OdsRecord> contractDataStream = allStream.filter(r -> "contract".equals(r.tableName));
        DataStream<OdsRecord> mainStream = allStream.filter(r -> !"contract".equals(r.tableName));

        // 按 credit_facility_id 分组，动态分流，输出 JSON 字符串
        SingleOutputStreamOperator<String> processStream = mainStream
                .keyBy(DwdBaseJob::extractKey)
                .process(new DwdProcessFunction())
                .disableChaining();

        // ============ 审批域：写 Doris + 双写 Kafka（供 DWS 实时消费） ============
        processStream.sinkTo(DimUtil.buildDorisSink("dwd.dwd_audit_approve", "label-audit-approve"));
        processStream.sinkTo(DimUtil.buildKafkaSink("dwd_audit_approve"));

        processStream.getSideOutput(DwdProcessFunction.REJECT_TAG)
                .sinkTo(DimUtil.buildDorisSink("dwd.dwd_audit_reject", "label-audit-reject"));
        processStream.getSideOutput(DwdProcessFunction.REJECT_TAG)
                .sinkTo(DimUtil.buildKafkaSink("dwd_audit_reject"));

        processStream.getSideOutput(DwdProcessFunction.CANCEL_TAG)
                .sinkTo(DimUtil.buildDorisSink("dwd.dwd_audit_cancel", "label-audit-cancel"));
        processStream.getSideOutput(DwdProcessFunction.CANCEL_TAG)
                .sinkTo(DimUtil.buildKafkaSink("dwd_audit_cancel"));

        // ============ 授信域：写 Doris + 双写 Kafka ============
        processStream.getSideOutput(DwdProcessFunction.CREDIT_ADD_TAG)
                .sinkTo(DimUtil.buildDorisSink("dwd.dwd_credit_add", "label-credit-add"));
        processStream.getSideOutput(DwdProcessFunction.CREDIT_ADD_TAG)
                .sinkTo(DimUtil.buildKafkaSink("dwd_credit_add"));

        processStream.getSideOutput(DwdProcessFunction.CREDIT_OCCUPY_TAG)
                .sinkTo(DimUtil.buildDorisSink("dwd.dwd_credit_occupy", "label-credit-occupy"));
        processStream.getSideOutput(DwdProcessFunction.CREDIT_OCCUPY_TAG)
                .sinkTo(DimUtil.buildKafkaSink("dwd_credit_occupy"));

        // ============ 租赁域：合同制作 ⋈ 合同表（签约/起租） ============
        SideOutputDataStream<String> produceStream = processStream.getSideOutput(DwdProcessFunction.CONTRACT_PRODUCE_TAG);
        produceStream.sinkTo(DimUtil.buildDorisSink("dwd.dwd_lease_contract_produce", "label-lease-produce"));
        produceStream.sinkTo(DimUtil.buildKafkaSink("dwd_lease_contract_produce"));

        // 合同制作按 credit_id 分组（从 JSON 里取 credit_id），合同表也按 credit_id 分组，双流关联
        SingleOutputStreamOperator<String> signedStream = produceStream
                .keyBy(DwdBaseJob::extractProduceKey)
                .connect(contractDataStream.keyBy(DwdBaseJob::extractContractKey))
                .process(new LeaseSignProcessFunction())
                .disableChaining();

        signedStream.sinkTo(DimUtil.buildDorisSink("dwd.dwd_lease_sign", "label-lease-sign"));
        signedStream.sinkTo(DimUtil.buildKafkaSink("dwd_lease_sign"));

        signedStream.getSideOutput(LeaseSignProcessFunction.EXECUTION_TAG)
                .sinkTo(DimUtil.buildDorisSink("dwd.dwd_lease_execution", "label-lease-execution"));
        signedStream.getSideOutput(LeaseSignProcessFunction.EXECUTION_TAG)
                .sinkTo(DimUtil.buildKafkaSink("dwd_lease_execution"));

        env.execute("DWD Base");
    }

    // ===================== 解析与字段提取 =====================

    private static DataStream<OdsRecord> readTopic(StreamExecutionEnvironment env, String topic, String groupId) {
        return env.fromSource(DimUtil.buildKafkaSource(topic, groupId), WatermarkStrategy.noWatermarks(), topic)
                .map(DwdBaseJob::parseOds)
                .filter(r -> r != null && r.after != null);
    }

    private static OdsRecord parseOds(String json) {
        try {
            JsonNode node = DimUtil.MAPPER.readTree(json);
            OdsRecord r = new OdsRecord();
            r.tableName = node.get("source").get("table").asText();
            r.op = node.get("op").asText();
            JsonNode after = node.get("after");
            r.after = (after == null || after.isNull()) ? null
                    : DimUtil.MAPPER.convertValue(after, new TypeReference<Map<String, Object>>() {});
            return r;
        } catch (Exception e) {
            return null;
        }
    }

    /** 分组键：credit_facility 用 id，其余用 credit_facility_id */
    private static String extractKey(OdsRecord r) {
        if ("credit_facility".equals(r.tableName)) {
            return String.valueOf(r.after.get("id"));
        }
        return String.valueOf(r.after.get("credit_facility_id"));
    }

    /** contract 表的分组键：credit_id */
    private static String extractContractKey(OdsRecord r) {
        return String.valueOf(r.after.get("credit_id"));
    }

    /** 合同制作 JSON 的分组键：credit_id（从 JSON 字符串里解析） */
    private static String extractProduceKey(String json) {
        try {
            JsonNode node = DimUtil.MAPPER.readTree(json);
            return String.valueOf(node.get("credit_id").asLong());
        } catch (Exception e) {
            return "0";
        }
    }
}
