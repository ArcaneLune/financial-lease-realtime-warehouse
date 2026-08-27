package com.flink.dws.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/** 授信域-新增授信窗口汇总（无维度，纯时间窗口） */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DwsCreditAddBean implements Serializable {
    @JsonProperty("stt") public String stt;
    @JsonProperty("edt") public String edt;
    @JsonProperty("cur_date") public String curDate;
    @JsonProperty("apply_count") public Long applyCount;
    @JsonProperty("apply_amount") public BigDecimal applyAmount;
    @JsonProperty("reply_amount") public BigDecimal replyAmount;
    @JsonProperty("credit_amount") public BigDecimal creditAmount;
    // 只反序列化（读入 add_time 供水位线用），序列化时不输出到 DWS 表
    // 注意：必须用 WRITE_ONLY（读入不输出）；READ_ONLY 是反的（只输出不读入），会导致 addTime 为 null
    @JsonProperty(value = "add_time", access = JsonProperty.Access.WRITE_ONLY)
    public String addTime;
    public DwsCreditAddBean() {}
}
