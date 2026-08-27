package com.flink.dws.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/** 审批域-信审经办粒度-审批拒绝窗口汇总 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DwsAuditAuditManRejectBean implements Serializable {
    @JsonProperty("stt") public String stt;
    @JsonProperty("edt") public String edt;
    @JsonProperty("cur_date") public String curDate;
    @JsonProperty("audit_man_id") public Long auditManId;
    @JsonProperty("audit_man_name") public String auditManName;
    @JsonProperty("apply_count") public Long applyCount;
    @JsonProperty("apply_amount") public BigDecimal applyAmount;
    @JsonProperty(value = "reject_time", access = JsonProperty.Access.WRITE_ONLY)
    public String rejectTime;
    public DwsAuditAuditManRejectBean() {}
}
