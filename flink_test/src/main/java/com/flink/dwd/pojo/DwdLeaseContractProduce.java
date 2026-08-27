package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 合同制作事实表（对应 Doris dwd.dwd_lease_contract_produce） */
public class DwdLeaseContractProduce implements Serializable {
    @JsonProperty("id") public long id;
    @JsonProperty("produced_time") public String producedTime;
    @JsonProperty("credit_id") public long creditId;
    @JsonProperty("credit_facility_id") public long creditFacilityId;
    @JsonProperty("apply_amount") public String applyAmount;
    @JsonProperty("reply_amount") public String replyAmount;
    @JsonProperty("credit_amount") public String creditAmount;
    public DwdLeaseContractProduce() {}
}
