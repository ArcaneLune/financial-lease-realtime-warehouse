package com.flink.dim.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 员工维度表（对应 Doris dim.dim_employee，原始镜像结构） */
public class DimEmployee implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("create_time")
    public String createTime;

    @JsonProperty("update_time")
    public String updateTime;

    @JsonProperty("name")
    public String name;

    @JsonProperty("type")
    public Long type;

    @JsonProperty("department_id")
    public Long departmentId;

    public DimEmployee() {
    }
}
