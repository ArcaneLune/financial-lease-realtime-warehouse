package com.flink.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.pojo.DimIndustry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 行业维度：Kafka(ods_industry) -> Doris(dim.dim_industry)
 * 原始镜像结构，不做三级拍平。
 */
public class DimIndustryJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 checkpoint：Doris sink 的 flush 依赖 checkpoint，否则小数据量会一直积压在 buffer 里不写入
        env.enableCheckpointing(10000);
        // 集群运行时 checkpoint 存到持久化目录（本地测试用默认 jobmanager 内存即可）
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> jsonStream = env.fromSource(
                DimUtil.buildKafkaSource("ods_industry", "dim_industry_group"),
                WatermarkStrategy.noWatermarks(),
                "ods_industry");

        DataStream<DimIndustry> industryStream = jsonStream
                .map(DimIndustryJob::parse)
                .filter(i -> i != null);

        industryStream.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dim.dim_industry", "label-ind"));

        env.execute("DIM Industry");
    }

    private static DimIndustry parse(String json) {
        JsonNode after = DimUtil.getAfter(json);
        if (after == null) {
            return null;
        }
        DimIndustry i = new DimIndustry();
        i.id = after.get("id").asLong();
        i.createTime = DimUtil.parseDatetime(after, "create_time");
        i.updateTime = DimUtil.parseDatetime(after, "update_time");
        i.industryLevel = DimUtil.getNullableInt(after, "industry_level");
        i.industryName = after.get("industry_name").asText();
        i.superiorIndustryId = DimUtil.getNullableLong(after, "superior_industry_id");
        return i;
    }
}
