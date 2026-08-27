package com.flink.dws.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/** 审批域-行业业务方向经办人粒度-审批拒绝窗口汇总（无 reply_amount） */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DwsAuditIndLeaseOrgSalesmanRejectBean implements Serializable {
    @JsonProperty("stt") public String stt;
    @JsonProperty("edt") public String edt;
    @JsonProperty("cur_date") public String curDate;
    @JsonProperty("industry1_id") public Long industry1Id;
    @JsonProperty("industry1_name") public String industry1Name;
    @JsonProperty("industry2_id") public Long industry2Id;
    @JsonProperty("industry2_name") public String industry2Name;
    @JsonProperty("industry3_id") public Long industry3Id;
    @JsonProperty("industry3_name") public String industry3Name;
    @JsonProperty("lease_organization") public String leaseOrganization;
    @JsonProperty("salesman_id") public Long salesmanId;
    @JsonProperty("salesman_name") public String salesmanName;
    @JsonProperty("department1_id") public Long department1Id;
    @JsonProperty("department1_name") public String department1Name;
    @JsonProperty("department2_id") public Long department2Id;
    @JsonProperty("department2_name") public String department2Name;
    @JsonProperty("department3_id") public Long department3Id;
    @JsonProperty("department3_name") public String department3Name;
    @JsonProperty("apply_count") public Long applyCount;
    @JsonProperty("apply_amount") public BigDecimal applyAmount;
    @JsonProperty(value = "reject_time", access = JsonProperty.Access.WRITE_ONLY)
    public String rejectTime;
    public DwsAuditIndLeaseOrgSalesmanRejectBean() {}
}
