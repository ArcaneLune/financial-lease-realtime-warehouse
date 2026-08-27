package com.flink.dwd.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 审批通过事实表（对应 Doris dwd.dwd_audit_approve） */
public class DwdAuditApprove implements Serializable {
    @JsonProperty("id") public long id;
    @JsonProperty("lease_organization") public String leaseOrganization;
    @JsonProperty("business_partner_id") public long businessPartnerId;
    @JsonProperty("industry_id") public long industryId;
    @JsonProperty("reply_id") public long replyId;
    @JsonProperty("salesman_id") public long salesmanId;
    @JsonProperty("audit_man_id") public Long auditManId;
    @JsonProperty("apply_amount") public String applyAmount;
    @JsonProperty("reply_amount") public String replyAmount;
    @JsonProperty("approve_time") public String approveTime;
    @JsonProperty("reply_time") public String replyTime;
    @JsonProperty("irr") public String irr;
    @JsonProperty("period") public Integer period;
    public DwdAuditApprove() {}
}
