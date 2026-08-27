package com.flink.dws;

import com.fasterxml.jackson.databind.JsonNode;
import com.flink.dim.DimUtil;
import com.flink.dws.bean.DimRow;
import com.flink.dws.bean.DwsAuditIndLeaseOrgSalesmanCancelBean;
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

/** DWS 审批域-行业业务方向经办人粒度-审批取消窗口汇总。读 dwd_audit_cancel → 窗口聚合 → 广播维度拍平 → 写 Doris。 */
public class DwsAuditIndLeaseOrgSalesmanCancelWin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/module/flink-1.17.1/checkpoints");

        DataStream<String> source = env.fromSource(
                DimUtil.buildKafkaSource("dwd_audit_cancel", "dws_audit_ind_lease_org_salesman_cancel_group"),
                WatermarkStrategy.noWatermarks(), "dwd_audit_cancel");

        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> beanStream = source.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanCancelBean map(String value) throws Exception {
                JsonNode node = DimUtil.MAPPER.readTree(value);
                DwsAuditIndLeaseOrgSalesmanCancelBean bean = new DwsAuditIndLeaseOrgSalesmanCancelBean();
                if (node.get("industry_id") != null) bean.industry3Id = node.get("industry_id").asLong();
                if (node.get("lease_organization") != null) bean.leaseOrganization = node.get("lease_organization").asText();
                if (node.get("salesman_id") != null) bean.salesmanId = node.get("salesman_id").asLong();
                if (node.get("apply_amount") != null) bean.applyAmount = new BigDecimal(node.get("apply_amount").asText());
                if (node.get("cancel_time") != null) bean.cancelTime = node.get("cancel_time").asText();
                bean.applyCount = 1L;
                return bean;
            }
        });

        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> withWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanCancelBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
                            @Override
                            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanCancelBean bean, long recordTimestamp) {
                                Long ts = DimUtil.strToTs(bean.cancelTime);
                                return ts == null ? 0L : ts;
                            }
                        }));

        KeyedStream<DwsAuditIndLeaseOrgSalesmanCancelBean, String> keyed = withWatermark
                .keyBy(bean -> bean.leaseOrganization + ":" + bean.industry3Id + ":" + bean.salesmanId);

        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> reduced = keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
                            @Override
                            public DwsAuditIndLeaseOrgSalesmanCancelBean reduce(DwsAuditIndLeaseOrgSalesmanCancelBean v1, DwsAuditIndLeaseOrgSalesmanCancelBean v2) {
                                v1.applyCount = v1.applyCount + v2.applyCount;
                                v1.applyAmount = v1.applyAmount.add(v2.applyAmount);
                                return v1;
                            }
                        },
                        new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<DwsAuditIndLeaseOrgSalesmanCancelBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanCancelBean> out) {
                                String stt = DimUtil.tsToYmdHms(context.window().getStart());
                                String edt = DimUtil.tsToYmdHms(context.window().getEnd());
                                String curDate = DimUtil.tsToDate(context.window().getStart());
                                for (DwsAuditIndLeaseOrgSalesmanCancelBean element : elements) {
                                    element.stt = stt;
                                    element.edt = edt;
                                    element.curDate = curDate;
                                    out.collect(element);
                                }
                            }
                        });

        BroadcastStream<DimRow> dimStream = DwsDimUtil.buildDimBroadcastStream(env).broadcast(DwsDimUtil.DIM_DESC);

        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> dimmed = reduced.connect(dimStream)
                .process(new BroadcastProcessFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, DimRow, DwsAuditIndLeaseOrgSalesmanCancelBean>() {
                    @Override
                    public void processBroadcastElement(DimRow row, Context ctx, Collector<DwsAuditIndLeaseOrgSalesmanCancelBean> out) throws Exception {
                        ctx.getBroadcastState(DwsDimUtil.DIM_DESC).put(row.table + ":" + row.id, row.data);
                    }

                    @Override
                    public void processElement(DwsAuditIndLeaseOrgSalesmanCancelBean bean, ReadOnlyContext ctx, Collector<DwsAuditIndLeaseOrgSalesmanCancelBean> out) throws Exception {
                        ReadOnlyBroadcastState<String, Map<String, Object>> dim = ctx.getBroadcastState(DwsDimUtil.DIM_DESC);
                        fillDim(bean, dim);
                        out.collect(bean);
                    }
                });

        dimmed.map(DimUtil::toJson)
                .sinkTo(DimUtil.buildDorisSink("dws.dws_audit_industry_lease_organization_salesman_cancel_win", "dws-audit-ind-lease-org-salesman-cancel"));

        env.execute("dws_audit_industry_lease_organization_salesman_cancel_win");
    }

    private static void fillDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean,
                                ReadOnlyBroadcastState<String, Map<String, Object>> dim) throws Exception {
        if (bean.industry3Id != null) {
            Map<String, Object> ind3 = dim.get("industry:" + bean.industry3Id);
            if (ind3 != null) {
                bean.industry3Name = str(ind3.get("industry_name"));
                Object sup = ind3.get("superior_industry_id");
                if (sup != null) {
                    bean.industry2Id = ((Number) sup).longValue();
                    Map<String, Object> ind2 = dim.get("industry:" + bean.industry2Id);
                    if (ind2 != null) {
                        bean.industry2Name = str(ind2.get("industry_name"));
                        Object sup1 = ind2.get("superior_industry_id");
                        if (sup1 != null) {
                            bean.industry1Id = ((Number) sup1).longValue();
                            Map<String, Object> ind1 = dim.get("industry:" + bean.industry1Id);
                            if (ind1 != null) bean.industry1Name = str(ind1.get("industry_name"));
                        }
                    }
                }
            }
        }
        if (bean.salesmanId != null) {
            Map<String, Object> emp = dim.get("employee:" + bean.salesmanId);
            if (emp != null) {
                bean.salesmanName = str(emp.get("name"));
                Object dep = emp.get("department_id");
                if (dep != null) {
                    bean.department3Id = ((Number) dep).longValue();
                    Map<String, Object> dep3 = dim.get("department:" + bean.department3Id);
                    if (dep3 != null) {
                        bean.department3Name = str(dep3.get("department_name"));
                        Object sup = dep3.get("superior_department_id");
                        if (sup != null) {
                            bean.department2Id = ((Number) sup).longValue();
                            Map<String, Object> dep2 = dim.get("department:" + bean.department2Id);
                            if (dep2 != null) {
                                bean.department2Name = str(dep2.get("department_name"));
                                Object sup1 = dep2.get("superior_department_id");
                                if (sup1 != null) {
                                    bean.department1Id = ((Number) sup1).longValue();
                                    Map<String, Object> dep1 = dim.get("department:" + bean.department1Id);
                                    if (dep1 != null) bean.department1Name = str(dep1.get("department_name"));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
