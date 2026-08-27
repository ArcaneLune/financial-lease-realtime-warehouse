package com.flink.dws.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/** 租赁域-签约窗口汇总（无维度） */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DwsLeaseSignBean implements Serializable {
    @JsonProperty("stt") public String stt;
    @JsonProperty("edt") public String edt;
    @JsonProperty("cur_date") public String curDate;
    @JsonProperty("apply_count") public Long applyCount;
    @JsonProperty("apply_amount") public BigDecimal applyAmount;
    @JsonProperty("reply_amount") public BigDecimal replyAmount;
    @JsonProperty("credit_amount") public BigDecimal creditAmount;
    @JsonProperty(value = "signed_time", access = JsonProperty.Access.WRITE_ONLY)
    public String signedTime;
    public DwsLeaseSignBean() {}
}
