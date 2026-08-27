package com.flink.dws;

import com.flink.dim.DimUtil;
import com.flink.dws.bean.DwsLeaseSignBean;
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

/** DWS 租赁域-签约窗口汇总（无维度）。读 dwd_lease_sign → 10秒窗口 reduce → 写 Doris。 */
public class DwsLeaseSignWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_lease_sign", "dws_lease_sign_group"),
                WatermarkStrategy.noWatermarks(), "dwd_lease_sign");

        SingleOutputStreamOperator<DwsLeaseSignBean> beanStream = source.map(new MapFunction<String, DwsLeaseSignBean>() {
            @Override
            public DwsLeaseSignBean map(String value) throws Exception {
                DwsLeaseSignBean bean = DimUtil.MAPPER.readValue(value, DwsLeaseSignBean.class);
                bean.applyCount = 1L;
                return bean;
            }
        });

        SingleOutputStreamOperator<DwsLeaseSignBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsLeaseSignBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsLeaseSignBean>() {
                            @Override
                            public long extractTimestamp(DwsLeaseSignBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.signedTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        AllWindowedStream<DwsLeaseSignBean, TimeWindow> windowed = withWatermark
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<DwsLeaseSignBean> reduced = windowed.reduce(
                new ReduceFunction<DwsLeaseSignBean>() {
                    @Override
                    public DwsLeaseSignBean reduce(DwsLeaseSignBean v1, DwsLeaseSignBean v2) {
                        v1.applyCount = v1.applyCount + v2.applyCount;
                        v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                        v1.replyAmount = v1.replyAmount.add(v2.replyAmount);
                        v1.creditAmount = v1.creditAmount.add(v2.creditAmount);
                        return v1;
                    }
                },
                new ProcessAllWindowFunction<DwsLeaseSignBean, DwsLeaseSignBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsLeaseSignBean> elements, Collector<DwsLeaseSignBean> out) {
                        String stt = DimUtil.tsToYmdHms(context.window().getStart());
                        String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                        String curDate = DimUtil.tsToDate(context.window().getStart());
                        for (DwsLeaseSignBean element : elements) {
                            element.stt = stt;
                            element.edt = edt;
                            element.curDate = curDate;
                            out.collect(element);
                        }
                    }
                });

        reduced.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_lease_sign_win", "dws-lease-sign"));

        env.execute("dws_lease_sign_win");
    }
}
