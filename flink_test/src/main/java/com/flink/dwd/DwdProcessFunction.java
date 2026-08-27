package com.flink.dwd;

import com.flink.dim.DimUtil;
import com.flink.dwd.pojo.DwdAuditApprove;
import com.flink.dwd.pojo.DwdAuditCancel;
import com.flink.dwd.pojo.DwdAuditReject;
import com.flink.dwd.pojo.DwdCreditAdd;
import com.flink.dwd.pojo.DwdCreditOccupy;
import com.flink.dwd.pojo.DwdLeaseContractProduce;
import com.flink.dwd.pojo.FacilityInfo;
import com.flink.dwd.pojo.ReplyDetail;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * DWD 层核心动态分流函数。
 *
 * 关键设计：
 * 1. 主输出 + 全部侧输出统一为 String（JSON），规避 Flink 异构 POJO 侧输出类型传播 bug（ClassCastException）。
 * 2. credit 事件由 credit 表生命周期触发，审批只补 apply/reply_amount，不卡住事件。
 * 3. 审批通过改由 credit_facility_status.status=16 检测（流转记录），而非 credit_facility.status=16（当前状态），
 *    避免漏掉已流转到 19「新增授信」的申请。
 *
 * 事件规则：
 *   审批通过 = credit_facility_status.status=16（出具批复审核通过）
 *   拒绝 = credit_facility.status=20；取消 = credit_facility.status=21（终态，当前状态即流转）
 *   audit_man_id（信审经办）= credit_facility_status.status∈(4,5,6) 的 employee_id
 *   新增授信 = credit 插入（op=c 或快照 op=r）
 *   授信占用 = credit_occupy_time 非空
 *   合同制作 = contract_produce_time 非空
 */
public class DwdProcessFunction extends KeyedProcessFunction<String, OdsRecord, String> {

    public static final OutputTag<String> REJECT_TAG = new OutputTag<String>("reject") {};
    public static final OutputTag<String> CANCEL_TAG = new OutputTag<String>("cancel") {};
    public static final OutputTag<String> CREDIT_ADD_TAG = new OutputTag<String>("credit_add") {};
    public static final OutputTag<String> CREDIT_OCCUPY_TAG = new OutputTag<String>("credit_occupy") {};
    public static final OutputTag<String> CONTRACT_PRODUCE_TAG = new OutputTag<String>("contract_produce") {};

    private ValueState<FacilityInfo> facilityState;          // credit_facility 核心字段
    private ValueState<Long> auditManIdState;                 // 信审经办
    private ValueState<ReplyDetail> replyState;               // 批复信息
    private ValueState<DwdAuditApprove> approvalState;        // 待发出的审批通过（id + approve_time）
    private ValueState<DwdAuditReject> rejectState;           // 待发出的拒绝
    private ValueState<DwdAuditCancel> cancelState;           // 待发出的取消
    private ValueState<DwdCreditAdd> creditAddState;          // 待发出的新增授信
    private ValueState<DwdCreditOccupy> creditOccupyState;    // 待发出的授信占用
    private ValueState<DwdLeaseContractProduce> contractProduceState; // 待发出的合同制作

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(1)).build();

        ValueStateDescriptor<FacilityInfo> fiDesc = new ValueStateDescriptor<>("facility", FacilityInfo.class);
        fiDesc.enableTimeToLive(ttl);
        facilityState = getRuntimeContext().getState(fiDesc);

        ValueStateDescriptor<Long> auditManDesc = new ValueStateDescriptor<>("audit_man_id", Long.class);
        auditManDesc.enableTimeToLive(ttl);
        auditManIdState = getRuntimeContext().getState(auditManDesc);

        ValueStateDescriptor<ReplyDetail> replyDesc = new ValueStateDescriptor<>("reply", ReplyDetail.class);
        replyDesc.enableTimeToLive(ttl);
        replyState = getRuntimeContext().getState(replyDesc);

        ValueStateDescriptor<DwdAuditApprove> approvalDesc = new ValueStateDescriptor<>("approval", DwdAuditApprove.class);
        approvalDesc.enableTimeToLive(ttl);
        approvalState = getRuntimeContext().getState(approvalDesc);

        ValueStateDescriptor<DwdAuditReject> rejectDesc = new ValueStateDescriptor<>("reject", DwdAuditReject.class);
        rejectDesc.enableTimeToLive(ttl);
        rejectState = getRuntimeContext().getState(rejectDesc);

        ValueStateDescriptor<DwdAuditCancel> cancelDesc = new ValueStateDescriptor<>("cancel", DwdAuditCancel.class);
        cancelDesc.enableTimeToLive(ttl);
        cancelState = getRuntimeContext().getState(cancelDesc);

        ValueStateDescriptor<DwdCreditAdd> addDesc = new ValueStateDescriptor<>("credit_add", DwdCreditAdd.class);
        addDesc.enableTimeToLive(ttl);
        creditAddState = getRuntimeContext().getState(addDesc);

        ValueStateDescriptor<DwdCreditOccupy> occupyDesc = new ValueStateDescriptor<>("credit_occupy", DwdCreditOccupy.class);
        occupyDesc.enableTimeToLive(ttl);
        creditOccupyState = getRuntimeContext().getState(occupyDesc);

        ValueStateDescriptor<DwdLeaseContractProduce> produceDesc = new ValueStateDescriptor<>("contract_produce", DwdLeaseContractProduce.class);
        produceDesc.enableTimeToLive(ttl);
        contractProduceState = getRuntimeContext().getState(produceDesc);
    }

    @Override
    public void processElement(OdsRecord rec, Context ctx, Collector<String> out) throws Exception {
        Map<String, Object> d = rec.after;
        if (d == null) {
            return;
        }
        String table = rec.tableName;
        if ("credit_facility_status".equals(table)) {
            int status = intVal(d, "status");
            if (status == 16) {
                // 审批通过事件（出具批复审核通过）：只记录 id + 通过时间，字段靠 facility 回填
                DwdAuditApprove approve = new DwdAuditApprove();
                approve.id = longVal(d, "credit_facility_id");
                approve.approveTime = datetime(d, "create_time");
                approvalState.update(approve);
                tryEmitApprove(ctx, out);
            } else if (status == 4 || status == 5 || status == 6) {
                // 信审经办（4 已分配 / 5 审核通过 / 6 审核复议）
                Long emp = nullableLong(d, "employee_id");
                if (emp != null) {
                    auditManIdState.update(emp);
                    tryEmitApprove(ctx, out);
                }
            } else if (status == 20) {
                // 拒绝（终态）：从流转记录取 id + 时间，字段靠 facility 回填
                DwdAuditReject reject = new DwdAuditReject();
                reject.id = longVal(d, "credit_facility_id");
                reject.rejectTime = datetime(d, "create_time");
                rejectState.update(reject);
                tryEmitReject(ctx);
            } else if (status == 21) {
                // 取消（终态）
                DwdAuditCancel cancel = new DwdAuditCancel();
                cancel.id = longVal(d, "credit_facility_id");
                cancel.cancelTime = datetime(d, "create_time");
                cancelState.update(cancel);
                tryEmitCancel(ctx);
            }
        } else if ("credit_facility".equals(table)) {
            facilityState.update(buildFacilityInfo(d));
            tryEmitApprove(ctx, out);
            tryEmitReject(ctx);
            tryEmitCancel(ctx);
            tryFlushCreditEvents(ctx);
        } else if ("reply".equals(table)) {
            replyState.update(buildReply(d));
            tryEmitApprove(ctx, out);
            tryFlushCreditEvents(ctx);
        } else if ("credit".equals(table)) {
            FacilityInfo fi = facilityState.value();
            String applyAmount = fi == null ? null : fi.applyAmount;
            String replyAmount = replyState.value() == null ? null : replyState.value().creditAmount;

            // 新增授信：credit 插入（快照 op=r，增量 op=c）
            if ("r".equals(rec.op) || "c".equals(rec.op)) {
                DwdCreditAdd add = buildCreditAdd(d);
                add.applyAmount = applyAmount;
                add.replyAmount = replyAmount;
                if (applyAmount == null || replyAmount == null) {
                    creditAddState.update(add);
                } else {
                    ctx.output(CREDIT_ADD_TAG, DimUtil.toJson(add));
                }
            }
            // 授信占用：credit_occupy_time 非空
            if (d.get("credit_occupy_time") != null) {
                DwdCreditOccupy occupy = buildCreditOccupy(d);
                occupy.applyAmount = applyAmount;
                occupy.replyAmount = replyAmount;
                if (applyAmount == null || replyAmount == null) {
                    creditOccupyState.update(occupy);
                } else {
                    ctx.output(CREDIT_OCCUPY_TAG, DimUtil.toJson(occupy));
                }
            }
            // 合同制作：contract_produce_time 非空
            if (d.get("contract_produce_time") != null) {
                DwdLeaseContractProduce produce = buildContractProduce(d);
                produce.applyAmount = applyAmount;
                produce.replyAmount = replyAmount;
                if (applyAmount == null || replyAmount == null) {
                    contractProduceState.update(produce);
                } else {
                    ctx.output(CONTRACT_PRODUCE_TAG, DimUtil.toJson(produce));
                }
            }
        }
    }

    /** 审批通过：status=16 + facility 字段 + 信审经办 + 批复都到齐才发出 */
    private void tryEmitApprove(Context ctx, Collector<String> out) throws Exception {
        DwdAuditApprove approve = approvalState.value();
        FacilityInfo fi = facilityState.value();
        Long auditMan = auditManIdState.value();
        ReplyDetail reply = replyState.value();
        if (approve == null || fi == null || auditMan == null || reply == null) {
            return;
        }
        approve.leaseOrganization = fi.leaseOrganization;
        approve.businessPartnerId = fi.businessPartnerId;
        approve.industryId = fi.industryId;
        approve.salesmanId = fi.salesmanId;
        approve.applyAmount = fi.applyAmount;
        approve.auditManId = auditMan;
        mergeReply(approve, reply);
        out.collect(DimUtil.toJson(approve));
        approvalState.clear();
    }

    private void tryEmitReject(Context ctx) throws Exception {
        DwdAuditReject reject = rejectState.value();
        FacilityInfo fi = facilityState.value();
        if (reject == null || fi == null) {
            return;
        }
        reject.leaseOrganization = fi.leaseOrganization;
        reject.businessPartnerId = fi.businessPartnerId;
        reject.industryId = fi.industryId;
        reject.salesmanId = fi.salesmanId;
        reject.applyAmount = fi.applyAmount;
        reject.auditManId = auditManIdState.value(); // 可能为 null（未分配信审经办就拒绝）
        ctx.output(REJECT_TAG, DimUtil.toJson(reject));
        rejectState.clear();
    }

    private void tryEmitCancel(Context ctx) throws Exception {
        DwdAuditCancel cancel = cancelState.value();
        FacilityInfo fi = facilityState.value();
        if (cancel == null || fi == null) {
            return;
        }
        cancel.leaseOrganization = fi.leaseOrganization;
        cancel.businessPartnerId = fi.businessPartnerId;
        cancel.industryId = fi.industryId;
        cancel.salesmanId = fi.salesmanId;
        cancel.applyAmount = fi.applyAmount;
        cancel.auditManId = auditManIdState.value(); // 可能为 null
        ctx.output(CANCEL_TAG, DimUtil.toJson(cancel));
        cancelState.clear();
    }

    /** credit 事件：申请金额 + 批复金额都到齐才发出 */
    private void tryFlushCreditEvents(Context ctx) throws Exception {
        FacilityInfo fi = facilityState.value();
        String replyAmount = replyState.value() == null ? null : replyState.value().creditAmount;
        if (fi == null || replyAmount == null) {
            return;
        }
        String applyAmount = fi.applyAmount;
        DwdCreditAdd add = creditAddState.value();
        if (add != null) {
            add.applyAmount = applyAmount;
            add.replyAmount = replyAmount;
            ctx.output(CREDIT_ADD_TAG, DimUtil.toJson(add));
            creditAddState.clear();
        }
        DwdCreditOccupy occupy = creditOccupyState.value();
        if (occupy != null) {
            occupy.applyAmount = applyAmount;
            occupy.replyAmount = replyAmount;
            ctx.output(CREDIT_OCCUPY_TAG, DimUtil.toJson(occupy));
            creditOccupyState.clear();
        }
        DwdLeaseContractProduce produce = contractProduceState.value();
        if (produce != null) {
            produce.applyAmount = applyAmount;
            produce.replyAmount = replyAmount;
            ctx.output(CONTRACT_PRODUCE_TAG, DimUtil.toJson(produce));
            contractProduceState.clear();
        }
    }

    private void mergeReply(DwdAuditApprove approve, ReplyDetail reply) {
        approve.replyId = reply.id;
        approve.replyAmount = reply.creditAmount;
        approve.replyTime = reply.createTime;
        approve.irr = reply.irr;
        approve.period = reply.period;
    }

    // ---------- 字段提取工具 ----------

    private static int intVal(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? 0 : ((Number) v).intValue();
    }

    private static long longVal(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? 0 : ((Number) v).longValue();
    }

    private static Long nullableLong(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : ((Number) v).longValue();
    }

    private static String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : String.valueOf(v);
    }

    private static String decimal(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : DimUtil.decodeDecimal(String.valueOf(v), 2);
    }

    private static String datetime(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : DimUtil.epochMicrosToStr(((Number) v).longValue());
    }

    // ---------- 构建各事实表 Bean ----------

    private static FacilityInfo buildFacilityInfo(Map<String, Object> d) {
        FacilityInfo fi = new FacilityInfo();
        fi.leaseOrganization = str(d, "lease_organization");
        fi.businessPartnerId = longVal(d, "business_partner_id");
        fi.industryId = longVal(d, "industry_id");
        fi.salesmanId = longVal(d, "salesman_id");
        fi.applyAmount = decimal(d, "credit_amount");
        return fi;
    }

    private static ReplyDetail buildReply(Map<String, Object> d) {
        ReplyDetail r = new ReplyDetail();
        r.id = longVal(d, "id");
        r.creditFacilityId = longVal(d, "credit_facility_id");
        r.creditAmount = decimal(d, "credit_amount");
        r.irr = decimal(d, "irr");
        Object period = d.get("period");
        r.period = period == null ? null : ((Number) period).intValue();
        r.createTime = datetime(d, "create_time");
        return r;
    }

    private static DwdCreditAdd buildCreditAdd(Map<String, Object> d) {
        DwdCreditAdd b = new DwdCreditAdd();
        b.id = longVal(d, "id");
        b.creditFacilityId = longVal(d, "credit_facility_id");
        b.addTime = datetime(d, "create_time");
        b.creditAmount = decimal(d, "credit_amount");
        return b;
    }

    private static DwdCreditOccupy buildCreditOccupy(Map<String, Object> d) {
        DwdCreditOccupy b = new DwdCreditOccupy();
        b.id = longVal(d, "id");
        b.creditFacilityId = longVal(d, "credit_facility_id");
        b.occupyTime = datetime(d, "credit_occupy_time");
        b.creditAmount = decimal(d, "credit_amount");
        return b;
    }

    private static DwdLeaseContractProduce buildContractProduce(Map<String, Object> d) {
        DwdLeaseContractProduce b = new DwdLeaseContractProduce();
        b.id = longVal(d, "contract_id");
        b.creditId = longVal(d, "id");
        b.creditFacilityId = longVal(d, "credit_facility_id");
        b.producedTime = datetime(d, "contract_produce_time");
        b.creditAmount = decimal(d, "credit_amount");
        return b;
    }
}
