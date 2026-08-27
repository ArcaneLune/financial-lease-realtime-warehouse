package com.flink.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.pojo.DimDepartment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 部门维度：Kafka(ods_department) -> Doris(dim.dim_department)
 * 原始镜像结构，不做三级拍平。
 */
public class DimDepartmentJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 checkpoint：Doris sink 的 flush 依赖 checkpoint，否则小数据量会一直积压在 buffer 里不写入
        env.enableCheckpointing(10000);
        // 集群运行时 checkpoint 存到持久化目录（本地测试用默认 jobmanager 内存即可）
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> jsonStream = env.fromSource(
                DimUtil.buildKafkaSource("ods_department", "dim_department_group"),
                WatermarkStrategy.noWatermarks(),
                "ods_department");

        DataStream<DimDepartment> deptStream = jsonStream
                .map(DimDepartmentJob::parse)
                .filter(d -> d != null);

        deptStream.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dim.dim_department", "label-dep"));

        env.execute("DIM Department");
    }

    private static DimDepartment parse(String json) {
        JsonNode after = DimUtil.getAfter(json);
        if (after == null) {
            return null;
        }
        DimDepartment d = new DimDepartment();
        d.id = after.get("id").asLong();
        d.createTime = DimUtil.parseDatetime(after, "create_time");
        d.updateTime = DimUtil.parseDatetime(after, "update_time");
        d.departmentLevel = DimUtil.getNullableInt(after, "department_level");
        d.departmentName = after.get("department_name").asText();
        d.superiorDepartmentId = DimUtil.getNullableLong(after, "superior_department_id");
        return d;
    }
}
