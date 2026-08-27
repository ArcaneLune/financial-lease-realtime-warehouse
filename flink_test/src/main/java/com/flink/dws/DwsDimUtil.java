package com.flink.dws;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.DimUtil;
import com.flink.dws.bean.DimRow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * DWS 层维度广播工具。
 * 从 ODS Kafka 读 3 张维度表（ods_industry / ods_employee / ods_department），
 * 统一解析成 {@link DimRow} 流，供各 DWS 作业 broadcast 后做维度拍平。
 * （替代参考代码的 HBase 异步 I/O，维度源用现成的 ODS topic，不用改 DIM 作业）
 */
public class DwsDimUtil {

    /** 广播状态：key = "表名:id"，value = 维度字段整行 Map（用 TypeHint 显式指定 Map<String,Object> 类型） */
    public static final MapStateDescriptor<String, Map<String, Object>> DIM_DESC =
            new MapStateDescriptor<>("dim_state",
                    TypeInformation.of(String.class),
                    new TypeHint<Map<String, Object>>() {}.getTypeInfo());

    /** 构建维度广播流：3 张维度表 union 成 DimRow 流 */
    public static DataStream<DimRow> buildDimBroadcastStream(StreamExecutionEnvironment env) {
        DataStream<DimRow> industry = readDim(env, "ods_industry", "dws_dim_industry_group");
        DataStream<DimRow> employee = readDim(env, "ods_employee", "dws_dim_employee_group");
        DataStream<DimRow> department = readDim(env, "ods_department", "dws_dim_department_group");
        return industry.union(employee, department);
    }

    private static DataStream<DimRow> readDim(StreamExecutionEnvironment env, String topic, String groupId) {
        return env.fromSource(DimUtil.buildKafkaSource(topic, groupId), WatermarkStrategy.noWatermarks(), topic)
                .map(DwsDimUtil::parseDim)
                .filter(r -> r != null);
    }

    /** 解析 ODS 维度 Debezium JSON → DimRow（快照/插入/更新都收，忽略删除） */
    private static DimRow parseDim(String json) {
        try {
            JsonNode node = DimUtil.MAPPER.readTree(json);
            if ("d".equals(node.get("op").asText())) {
                return null;
            }
            JsonNode after = node.get("after");
            if (after == null || after.isNull()) {
                return null;
            }
            Map<String, Object> data = DimUtil.MAPPER.convertValue(after, new TypeReference<Map<String, Object>>() {});
            if (data.get("id") == null) {
                return null;
            }
            DimRow row = new DimRow();
            row.table = node.get("source").get("table").asText();
            row.id = ((Number) data.get("id")).longValue();
            row.data = data;
            return row;
        } catch (Exception e) {
            return null;
        }
    }
}
