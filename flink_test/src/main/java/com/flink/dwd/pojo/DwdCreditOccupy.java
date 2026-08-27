package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 授信占用事实表（对应 Doris dwd.dwd_credit_occupy） */
public class DwdCreditOccupy implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("occupy_time")
    public String occupyTime;

    @JsonProperty("credit_facility_id")
    public long creditFacilityId;

    @JsonProperty("apply_amount")
    public String applyAmount;

    @JsonProperty("reply_amount")
    public String replyAmount;

    @JsonProperty("credit_amount")
    public String creditAmount;

    public DwdCreditOccupy() {
    }
}
