package com.flink.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.pojo.DimEmployee;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 员工维度：Kafka(ods_employee) -> Doris(dim.dim_employee)
 * 原始镜像结构，department_id 原样保留。
 */
public class DimEmployeeJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 checkpoint：Doris sink 的 flush 依赖 checkpoint，否则小数据量会一直积压在 buffer 里不写入
        env.enableCheckpointing(10000);
        // 集群运行时 checkpoint 存到持久化目录（本地测试用默认 jobmanager 内存即可）
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> jsonStream = env.fromSource(
                DimUtil.buildKafkaSource("ods_employee", "dim_employee_group"),
                WatermarkStrategy.noWatermarks(),
                "ods_employee");

        DataStream<DimEmployee> employeeStream = jsonStream
                .map(DimEmployeeJob::parse)
                .filter(e -> e != null);

        employeeStream.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dim.dim_employee", "label-emp"));

        env.execute("DIM Employee");
    }

    private static DimEmployee parse(String json) {
        JsonNode after = DimUtil.getAfter(json);
        if (after == null) {
            return null;
        }
        DimEmployee e = new DimEmployee();
        e.id = after.get("id").asLong();
        e.createTime = DimUtil.parseDatetime(after, "create_time");
        e.updateTime = DimUtil.parseDatetime(after, "update_time");
        e.name = after.get("name").asText();
        e.type = DimUtil.getNullableLong(after, "type");
        e.departmentId = DimUtil.getNullableLong(after, "department_id");
        return e;
    }
}
