package com.flink.dws;

import com.flink.dim.DimUtil;
import com.flink.dws.bean.DwsCreditOccupyBean;
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

/** DWS 授信域-授信占用窗口汇总（无维度）。读 dwd_credit_occupy → 10秒窗口 reduce → 写 Doris。 */
public class DwsCreditCreditOccupyWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_credit_occupy", "dws_credit_occupy_group"),
                WatermarkStrategy.noWatermarks(), "dwd_credit_occupy");

        SingleOutputStreamOperator<DwsCreditOccupyBean> beanStream = source.map(new MapFunction<String, DwsCreditOccupyBean>() {
            @Override
            public DwsCreditOccupyBean map(String value) throws Exception {
                DwsCreditOccupyBean bean = DimUtil.MAPPER.readValue(value, DwsCreditOccupyBean.class);
                bean.applyCount = 1L;
                return bean;
            }
        });

        SingleOutputStreamOperator<DwsCreditOccupyBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsCreditOccupyBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsCreditOccupyBean>() {
                            @Override
                            public long extractTimestamp(DwsCreditOccupyBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.occupyTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        AllWindowedStream<DwsCreditOccupyBean, TimeWindow> windowed = withWatermark
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<DwsCreditOccupyBean> reduced = windowed.reduce(
                new ReduceFunction<DwsCreditOccupyBean>() {
                    @Override
                    public DwsCreditOccupyBean reduce(DwsCreditOccupyBean v1, DwsCreditOccupyBean v2) {
                        v1.applyCount = v1.applyCount + v2.applyCount;
                        v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                        v1.replyAmount = v1.replyAmount.add(v2.replyAmount);
                        v1.creditAmount = v1.creditAmount.add(v2.creditAmount);
                        return v1;
                    }
                },
                new ProcessAllWindowFunction<DwsCreditOccupyBean, DwsCreditOccupyBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsCreditOccupyBean> elements, Collector<DwsCreditOccupyBean> out) {
                        String stt = DimUtil.tsToYmdHms(context.window().getStart());
                        String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                        String curDate = DimUtil.tsToDate(context.window().getStart());
                        for (DwsCreditOccupyBean element : elements) {
                            element.stt = stt;
                            element.edt = edt;
                            element.curDate = curDate;
                            out.collect(element);
                        }
                    }
                });

        reduced.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_credit_credit_occupy_win", "dws-credit-occupy"));

        env.execute("dws_credit_credit_occupy_win");
    }
}
