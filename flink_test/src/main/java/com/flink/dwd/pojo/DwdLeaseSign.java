package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 合同签约事实表（对应 Doris dwd.dwd_lease_sign） */
public class DwdLeaseSign implements Serializable {
    @JsonProperty("id") public long id;
    @JsonProperty("signed_time") public String signedTime;
    @JsonProperty("credit_id") public long creditId;
    @JsonProperty("credit_facility_id") public long creditFacilityId;
    @JsonProperty("apply_amount") public String applyAmount;
    @JsonProperty("reply_amount") public String replyAmount;
    @JsonProperty("credit_amount") public String creditAmount;
    public DwdLeaseSign() {}
}
