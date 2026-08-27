package com.flink.dws;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.DimUtil;
import com.flink.dws.bean.DimRow;
import com.flink.dws.bean.DwsAuditAuditManApprovalBean;
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

/** DWS 审批域-信审经办粒度-审批通过窗口汇总。读 dwd_audit_approve → 按信审经办聚合 → 广播 employee 补姓名 → 写 Doris。 */
public class DwsAuditAuditManApprovalWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_audit_approve", "dws_audit_audit_man_approval_group"),
                WatermarkStrategy.noWatermarks(), "dwd_audit_approve");

        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> beanStream = source.map(new MapFunction<String, DwsAuditAuditManApprovalBean>() {
            @Override
            public DwsAuditAuditManApprovalBean map(String value) throws Exception {
                JsonNode node = DimUtil.MAPPER.readTree(value);
                DwsAuditAuditManApprovalBean bean = new DwsAuditAuditManApprovalBean();
                if (node.get("audit_man_id") != null && !node.get("audit_man_id").isNull()) {
                    bean.auditManId = node.get("audit_man_id").asLong();
                }
                if (node.get("apply_amount") != null) bean.applyAmount = new BigDecimal(node.get("apply_amount").asText());
                if (node.get("reply_amount") != null) bean.replyAmount = new BigDecimal(node.get("reply_amount").asText());
                if (node.get("approve_time") != null) bean.approveTime = node.get("approve_time").asText();
                bean.applyCount = 1L;
                return bean;
            }
        }).filter(bean -> bean.auditManId != null);

        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsAuditAuditManApprovalBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManApprovalBean>() {
                            @Override
                            public long extractTimestamp(DwsAuditAuditManApprovalBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.approveTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        KeyedStream<DwsAuditAuditManApprovalBean, Long> keyed = withWatermark.keyBy(bean -> bean.auditManId);

        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> reduced = keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsAuditAuditManApprovalBean>() {
                            @Override
                            public DwsAuditAuditManApprovalBean reduce(DwsAuditAuditManApprovalBean v1, DwsAuditAuditManApprovalBean v2) {
                                v1.applyCount = v1.applyCount + v2.applyCount;
                                v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                                v1.replyAmount = v1.replyAmount.add(v2.replyAmount);
                                return v1;
                            }
                        },
                        new ProcessWindowFunction<DwsAuditAuditManApprovalBean, DwsAuditAuditManApprovalBean, Long, TimeWindow>() {
                            @Override
                            public void process(Long key, Context context, Iterable<DwsAuditAuditManApprovalBean> elements, Collector<DwsAuditAuditManApprovalBean> out) {
                                String stt = DimUtil.tsToYmdHms(context.window().getStart());
                                String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                                String curDate = DimUtil.tsToDate(context.window().getStart());
                                for (DwsAuditAuditManApprovalBean element : elements) {
                                    element.stt = stt;
                                    element.edt = edt;
                                    element.curDate = curDate;
                                    out.collect(element);
                                }
                            }
                        });

        BroadcastStream<DimRow> dimStream = DwsDimUtil.buildDimBroadcastStream(env).broadcast(DwsDimUtil.DIM_DESC);

        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> dimmed = reduced.connect(dimStream)
                .process(new BroadcastProcessFunction<DwsAuditAuditManApprovalBean, DimRow, DwsAuditAuditManApprovalBean>() {
                    @Override
                    public void processBroadcastElement(DimRow row, Context ctx, Collector<DwsAuditAuditManApprovalBean> out) throws Exception {
                        ctx.getBroadcastState(DwsDimUtil.DIM_DESC).put(row.table + ":" + row.id, row.data);
                    }

                    @Override
                    public void processElement(DwsAuditAuditManApprovalBean bean, ReadOnlyContext ctx, Collector<DwsAuditAuditManApprovalBean> out) throws Exception {
                        ReadOnlyBroadcastState<String, Map<String, Object>> dim = ctx.getBroadcastState(DwsDimUtil.DIM_DESC);
                        Map<String, Object> emp = dim.get("employee:" + bean.auditManId);
                        if (emp != null) {
                            bean.auditManName = str(emp.get("name"));
                        }
                        out.collect(bean);
                    }
                });

        dimmed.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_audit_audit_man_approval_win", "dws-audit-audit-man-approval"));

        env.execute("dws_audit_audit_man_approval_win");
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
