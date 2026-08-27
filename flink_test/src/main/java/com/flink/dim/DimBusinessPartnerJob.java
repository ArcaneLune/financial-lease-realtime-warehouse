package com.flink.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.pojo.DimBusinessPartner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 商业合伙人维度：Kafka(ods_business_partner) -> Doris(dim.dim_business_partner)
 */
public class DimBusinessPartnerJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 checkpoint：Doris sink 的 flush 依赖 checkpoint，否则小数据量会一直积压在 buffer 里不写入
        env.enableCheckpointing(10000);
        // 集群运行时 checkpoint 存到持久化目录（本地测试用默认 jobmanager 内存即可）
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> jsonStream = env.fromSource(
                DimUtil.buildKafkaSource("ods_business_partner", "dim_business_partner_group"),
                WatermarkStrategy.noWatermarks(),
                "ods_business_partner");

        DataStream<DimBusinessPartner> bpStream = jsonStream
                .map(DimBusinessPartnerJob::parse)
                .filter(bp -> bp != null);

        bpStream.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dim.dim_business_partner", "label-bp"));

        env.execute("DIM BusinessPartner");
    }

    private static DimBusinessPartner parse(String json) {
        JsonNode after = DimUtil.getAfter(json);
        if (after == null) {
            return null;
        }
        DimBusinessPartner bp = new DimBusinessPartner();
        bp.id = after.get("id").asLong();
        bp.createTime = DimUtil.parseDatetime(after, "create_time");
        bp.updateTime = DimUtil.parseDatetime(after, "update_time");
        bp.name = after.get("name").asText();
        return bp;
    }
}
