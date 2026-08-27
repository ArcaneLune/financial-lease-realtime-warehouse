package com.flink.dwd.pojo;

import java.io.Serializable;

/**
 * credit_facility 表的核心字段快照（供审批通过/授信事件回填）。
 * 审批通过改由 credit_facility_status.status=16 检测，但 lease_organization 等字段
 * 只存在于 credit_facility 表，需按 credit_facility_id 单独关联，故用此 POJO 存中间状态。
 */
public class FacilityInfo implements Serializable {
    public String leaseOrganization;
    public long businessPartnerId;
    public long industryId;
    public long salesmanId;
    public String applyAmount; // credit_facility.credit_amount（申请授信金额）

    public FacilityInfo() {}
}
