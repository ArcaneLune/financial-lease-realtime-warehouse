package com.flink.dwd.pojo;

import java.io.Serializable;

/** 批复表完整字段（ods_reply，审批通过事实表的主流） */
public class ReplyDetail implements Serializable {

    public long id;
    public long creditFacilityId;
    public String creditAmount;  // 批复金额
    public String irr;
    public Integer period;
    public String createTime;    // 批复时间

    public ReplyDetail() {
    }
}
