package com.flink.dws;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.DimUtil;
import com.flink.dws.bean.DimRow;
import com.flink.dws.bean.DwsAuditAuditManRejectBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/** DWS 审批域-信审经办粒度-审批拒绝窗口汇总。读 dwd_audit_reject → 按信审经办聚合 → 广播 employee 补姓名 → 写 Doris。 */
public class DwsAuditAuditManRejectWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_audit_reject", "dws_audit_audit_man_reject_group"),
                WatermarkStrategy.noWatermarks(), "dwd_audit_reject");

        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> beanStream = source.map(new MapFunction<String, DwsAuditAuditManRejectBean>() {
            @Override
            public DwsAuditAuditManRejectBean map(String value) throws Exception {
                JsonNode node = DimUtil.MAPPER.readTree(value);
                DwsAuditAuditManRejectBean bean = new DwsAuditAuditManRejectBean();
                if (node.get("audit_man_id") != null && !node.get("audit_man_id").isNull()) {
                    bean.auditManId = node.get("audit_man_id").asLong();
                }
                if (node.get("apply_amount") != null) bean.applyAmount = new BigDecimal(node.get("apply_amount").asText());
                if (node.get("reject_time") != null) bean.rejectTime = node.get("reject_time").asText();
                bean.applyCount = 1L;
                return bean;
            }
        }).filter(bean -> bean.auditManId != null);

        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsAuditAuditManRejectBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManRejectBean>() {
                            @Override
                            public long extractTimestamp(DwsAuditAuditManRejectBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.rejectTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        KeyedStream<DwsAuditAuditManRejectBean, Long> keyed = withWatermark.keyBy(bean -> bean.auditManId);

        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> reduced = keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsAuditAuditManRejectBean>() {
                            @Override
                            public DwsAuditAuditManRejectBean reduce(DwsAuditAuditManRejectBean v1, DwsAuditAuditManRejectBean v2) {
                                v1.applyCount = v1.applyCount + v2.applyCount;
                                v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                                return v1;
                            }
                        },
                        new ProcessWindowFunction<DwsAuditAuditManRejectBean, DwsAuditAuditManRejectBean, Long, TimeWindow>() {
                            @Override
                            public void process(Long key, Context context, Iterable<DwsAuditAuditManRejectBean> elements, Collector<DwsAuditAuditManRejectBean> out) {
                                String stt = DimUtil.tsToYmdHms(context.window().getStart());
                                String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                                String curDate = DimUtil.tsToDate(context.window().getStart());
                                for (DwsAuditAuditManRejectBean element : elements) {
                                    element.stt = stt;
                                    element.edt = edt;
                                    element.curDate = curDate;
                                    out.collect(element);
                                }
                            }
                        });

        BroadcastStream<DimRow> dimStream = DwsDimUtil.buildDimBroadcastStream(env).broadcast(DwsDimUtil.DIM_DESC);

        SingleOutputStreamOperator<DwsAuditAuditManRejectBean> dimmed = reduced.connect(dimStream)
                .process(new BroadcastProcessFunction<DwsAuditAuditManRejectBean, DimRow, DwsAuditAuditManRejectBean>() {
                    @Override
                    public void processBroadcastElement(DimRow row, Context ctx, Collector<DwsAuditAuditManRejectBean> out) throws Exception {
                        ctx.getBroadcastState(DwsDimUtil.DIM_DESC).put(row.table + ":" + row.id, row.data);
                    }

                    @Override
                    public void processElement(DwsAuditAuditManRejectBean bean, ReadOnlyContext ctx, Collector<DwsAuditAuditManRejectBean> out) throws Exception {
                        ReadOnlyBroadcastState<String, Map<String, Object>> dim = ctx.getBroadcastState(DwsDimUtil.DIM_DESC);
                        Map<String, Object> emp = dim.get("employee:" + bean.auditManId);
                        if (emp != null) {
                            bean.auditManName = str(emp.get("name"));
                        }
                        out.collect(bean);
                    }
                });

        dimmed.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_audit_audit_man_reject_win", "dws-audit-audit-man-reject"));

        env.execute("dws_audit_audit_man_reject_win");
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
