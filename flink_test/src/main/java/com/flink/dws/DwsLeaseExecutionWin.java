package com.flink.dws;

import com.flink.dim.DimUtil;
import com.flink.dws.bean.DwsLeaseExecutionBean;
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

/** DWS 租赁域-起租窗口汇总（无维度）。读 dwd_lease_execution → 10秒窗口 reduce → 写 Doris。 */
public class DwsLeaseExecutionWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_lease_execution", "dws_lease_execution_group"),
                WatermarkStrategy.noWatermarks(), "dwd_lease_execution");

        SingleOutputStreamOperator<DwsLeaseExecutionBean> beanStream = source.map(new MapFunction<String, DwsLeaseExecutionBean>() {
            @Override
            public DwsLeaseExecutionBean map(String value) throws Exception {
                DwsLeaseExecutionBean bean = DimUtil.MAPPER.readValue(value, DwsLeaseExecutionBean.class);
                bean.applyCount = 1L;
                return bean;
            }
        });

        SingleOutputStreamOperator<DwsLeaseExecutionBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsLeaseExecutionBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsLeaseExecutionBean>() {
                            @Override
                            public long extractTimestamp(DwsLeaseExecutionBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.executionTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        AllWindowedStream<DwsLeaseExecutionBean, TimeWindow> windowed = withWatermark
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<DwsLeaseExecutionBean> reduced = windowed.reduce(
                new ReduceFunction<DwsLeaseExecutionBean>() {
                    @Override
                    public DwsLeaseExecutionBean reduce(DwsLeaseExecutionBean v1, DwsLeaseExecutionBean v2) {
                        v1.applyCount = v1.applyCount + v2.applyCount;
                        v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                        v1.replyAmount = v1.replyAmount.add(v2.replyAmount);
                        v1.creditAmount = v1.creditAmount.add(v2.creditAmount);
                        return v1;
                    }
                },
                new ProcessAllWindowFunction<DwsLeaseExecutionBean, DwsLeaseExecutionBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsLeaseExecutionBean> elements, Collector<DwsLeaseExecutionBean> out) {
                        String stt = DimUtil.tsToYmdHms(context.window().getStart());
                        String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                        String curDate = DimUtil.tsToDate(context.window().getStart());
                        for (DwsLeaseExecutionBean element : elements) {
                            element.stt = stt;
                            element.edt = edt;
                            element.curDate = curDate;
                            out.collect(element);
                        }
                    }
                });

        reduced.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_lease_execution_win", "dws-lease-execution"));

        env.execute("dws_lease_execution_win");
    }
}
