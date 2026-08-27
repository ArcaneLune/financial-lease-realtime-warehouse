package com.flink.dwd;

import com.flink.dim.DimUtil;
import com.flink.dwd.pojo.DwdLeaseContractProduce;
import com.flink.dwd.pojo.DwdLeaseExecution;
import com.flink.dwd.pojo.DwdLeaseSign;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * 租赁域双流关联：合同制作（credit 表产出）⋈ 合同表（签约/起租），按 credit_id 关联。
 * 谁先到谁等：合同制作先到存 produceState，签约/起租先到存 signState/executionState。
 *
 * 关键设计：输入/输出统一为 String（JSON），与 {@link DwdProcessFunction} 一致，
 * 规避 Flink 异构 POJO 侧输出的类型传播 bug（ClassCastException）。
 * 业务状态仍在内部用 POJO ValueState 维护，emit 时转 JSON。
 */
public class LeaseSignProcessFunction extends KeyedCoProcessFunction<String, String, OdsRecord, String> {

    public static final OutputTag<String> EXECUTION_TAG = new OutputTag<String>("execution") {};

    private ValueState<DwdLeaseContractProduce> produceState;
    private ValueState<DwdLeaseSign> signState;
    private ValueState<DwdLeaseExecution> executionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        produceState = getRuntimeContext().getState(new ValueStateDescriptor<>("produce", DwdLeaseContractProduce.class));
        signState = getRuntimeContext().getState(new ValueStateDescriptor<>("sign", DwdLeaseSign.class));
        executionState = getRuntimeContext().getState(new ValueStateDescriptor<>("execution", DwdLeaseExecution.class));
    }

    @Override
    public void processElement1(String produceJson, Context ctx, Collector<String> out) throws Exception {
        DwdLeaseContractProduce produce = parseProduce(produceJson);
        if (produce == null) {
            return;
        }
        DwdLeaseSign sign = signState.value();
        DwdLeaseExecution execution = executionState.value();
        if (sign == null || execution == null) {
            produceState.update(produce); // 存合同制作数据，等签约/起租
        }
        if (sign != null) {
            fillAmounts(sign, produce);
            out.collect(DimUtil.toJson(sign));
            signState.clear();
        }
        if (execution != null) {
            fillAmounts(execution, produce);
            ctx.output(EXECUTION_TAG, DimUtil.toJson(execution));
            executionState.clear();
        }
    }

    @Override
    public void processElement2(OdsRecord rec, Context ctx, Collector<String> out) throws Exception {
        Map<String, Object> d = rec.after;
        if (d == null) {
            return;
        }
        DwdLeaseContractProduce produce = produceState.value();
        // 签约：signed_time 非空（含已起租的合同，它们也曾签约）
        if (d.get("signed_time") != null) {
            DwdLeaseSign sign = new DwdLeaseSign();
            sign.id = longVal(d, "id");
            sign.creditId = longVal(d, "credit_id");
            sign.signedTime = datetime(d, "signed_time");
            if (produce == null) {
                signState.update(sign); // 存签约数据，等合同制作
            } else {
                fillAmounts(sign, produce);
                out.collect(DimUtil.toJson(sign));
            }
        }
        // 起租：execution_time 非空
        if (d.get("execution_time") != null) {
            DwdLeaseExecution execution = new DwdLeaseExecution();
            execution.id = longVal(d, "id");
            execution.creditId = longVal(d, "credit_id");
            execution.executionTime = datetime(d, "execution_time");
            if (produce == null) {
                executionState.update(execution); // 存起租数据，等合同制作
            } else {
                fillAmounts(execution, produce);
                ctx.output(EXECUTION_TAG, DimUtil.toJson(execution));
            }
        }
    }

    private DwdLeaseContractProduce parseProduce(String json) {
        try {
            return DimUtil.MAPPER.readValue(json, DwdLeaseContractProduce.class);
        } catch (Exception e) {
            return null;
        }
    }

    private void fillAmounts(DwdLeaseSign sign, DwdLeaseContractProduce produce) {
        sign.creditFacilityId = produce.creditFacilityId;
        sign.applyAmount = produce.applyAmount;
        sign.replyAmount = produce.replyAmount;
        sign.creditAmount = produce.creditAmount;
    }

    private void fillAmounts(DwdLeaseExecution execution, DwdLeaseContractProduce produce) {
        execution.creditFacilityId = produce.creditFacilityId;
        execution.applyAmount = produce.applyAmount;
        execution.replyAmount = produce.replyAmount;
        execution.creditAmount = produce.creditAmount;
    }

    // ---------- 字段提取 ----------
    private static long longVal(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? 0 : ((Number) v).longValue();
    }

    private static String datetime(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : DimUtil.epochMicrosToStr(((Number) v).longValue());
    }
}
