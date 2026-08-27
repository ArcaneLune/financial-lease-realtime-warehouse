package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 审批拒绝事实表（对应 Doris dwd.dwd_audit_reject） */
public class DwdAuditReject implements Serializable {
    @JsonProperty("id") public long id;
    @JsonProperty("lease_organization") public String leaseOrganization;
    @JsonProperty("business_partner_id") public long businessPartnerId;
    @JsonProperty("industry_id") public long industryId;
    @JsonProperty("salesman_id") public long salesmanId;
    @JsonProperty("audit_man_id") public Long auditManId;
    @JsonProperty("apply_amount") public String applyAmount;
    @JsonProperty("reject_time") public String rejectTime;
    public DwdAuditReject() {}
}
