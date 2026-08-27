package com.flink.dim.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 商业合伙人（客户）维度表（对应 Doris dim.dim_business_partner） */
public class DimBusinessPartner implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("create_time")
    public String createTime;

    @JsonProperty("update_time")
    public String updateTime;

    @JsonProperty("name")
    public String name;

    public DimBusinessPartner() {
    }
}
