package com.flink.dws;

import com.flink.dim.DimUtil;
import com.flink.dws.bean.DwsCreditAddBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS 授信域-新增授信窗口汇总（无维度，纯时间窗口）。
 * 读 DWD 双写的 Kafka topic dwd_credit_add → 事件时间水位线 → 10 秒滚动窗口 reduce 累加 → 写 Doris。
 */
public class DwsCreditCreditAddWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        // 1. 从 Kafka 读 DWD 双写的新增授信数据
        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_credit_add", "dws_credit_add_group"),
                WatermarkStrategy.noWatermarks(), "dwd_credit_add");

        // 2. JSON → Bean
        SingleOutputStreamOperator<DwsCreditAddBean> beanStream = source.map(new MapFunction<String, DwsCreditAddBean>() {
            @Override
            public DwsCreditAddBean map(String value) throws Exception {
                DwsCreditAddBean bean = DimUtil.MAPPER.readValue(value, DwsCreditAddBean.class);
                bean.applyCount = 1L;
                return bean;
            }
        });

        // 3. 事件时间水位线（基于 add_time）
        SingleOutputStreamOperator<DwsCreditAddBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsCreditAddBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsCreditAddBean>() {
                            @Override
                            public long extractTimestamp(DwsCreditAddBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.addTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        // 4. 无维度，windowAll 10 秒滚动窗口
        AllWindowedStream<DwsCreditAddBean, TimeWindow> windowed = withWatermark
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 5. reduce 累加 + 补窗口时间
        SingleOutputStreamOperator<DwsCreditAddBean> reduced = windowed.reduce(
                new ReduceFunction<DwsCreditAddBean>() {
                    @Override
                    public DwsCreditAddBean reduce(DwsCreditAddBean v1, DwsCreditAddBean v2) {
                        v1.applyCount = v1.applyCount + v2.applyCount;
                        v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                        v1.replyAmount = v1.replyAmount.add(v2.replyAmount);
                        v1.creditAmount = v1.creditAmount.add(v2.creditAmount);
                        return v1;
                    }
                },
                new ProcessAllWindowFunction<DwsCreditAddBean, DwsCreditAddBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsCreditAddBean> elements, Collector<DwsCreditAddBean> out) {
                        String stt = DimUtil.tsToYmdHms(context.window().getStart());
                        String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                        String curDate = DimUtil.tsToDate(context.window().getStart());
                        for (DwsCreditAddBean element : elements) {
                            element.stt = stt;
                            element.edt = edt;
                            element.curDate = curDate;
                            out.collect(element);
                        }
                    }
                });

        // 6. 转 JSON 并写 Doris（print 用于排查数据流到哪一步）
        SingleOutputStreamOperator<String> jsonStream = reduced.map(DimUtil::toJson);
        jsonStream.sinkTo(DimUtil.buildDorisSink("dws.dws_credit_credit_add_win", "dws-credit-add"));

        env.execute("dws_credit_credit_add_win");
    }
}
