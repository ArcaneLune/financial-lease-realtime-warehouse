package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 合同起租事实表（对应 Doris dwd.dwd_lease_execution） */
public class DwdLeaseExecution implements Serializable {
    @JsonProperty("id") public long id;
    @JsonProperty("execution_time") public String executionTime;
    @JsonProperty("credit_id") public long creditId;
    @JsonProperty("credit_facility_id") public long creditFacilityId;
    @JsonProperty("apply_amount") public String applyAmount;
    @JsonProperty("reply_amount") public String replyAmount;
    @JsonProperty("credit_amount") public String creditAmount;
    public DwdLeaseExecution() {}
}
