package com.flink.dim.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 行业维度表（对应 Doris dim.dim_industry，原始镜像结构） */
public class DimIndustry implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("create_time")
    public String createTime;

    @JsonProperty("update_time")
    public String updateTime;

    @JsonProperty("industry_level")
    public Integer industryLevel;

    @JsonProperty("industry_name")
    public String industryName;

    @JsonProperty("superior_industry_id")
    public Long superiorIndustryId;

    public DimIndustry() {
    }
}
