package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 新增授信事实表（对应 Doris dwd.dwd_credit_add） */
public class DwdCreditAdd implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("add_time")
    public String addTime;

    @JsonProperty("credit_facility_id")
    public long creditFacilityId;

    @JsonProperty("apply_amount")
    public String applyAmount;

    @JsonProperty("reply_amount")
    public String replyAmount;

    @JsonProperty("credit_amount")
    public String creditAmount;

    public DwdCreditAdd() {
    }
}
