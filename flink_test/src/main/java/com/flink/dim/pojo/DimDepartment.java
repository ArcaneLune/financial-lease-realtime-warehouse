package com.flink.dim.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** 部门维度表（对应 Doris dim.dim_department，原始镜像结构） */
public class DimDepartment implements Serializable {

    @JsonProperty("id")
    public long id;

    @JsonProperty("create_time")
    public String createTime;

    @JsonProperty("update_time")
    public String updateTime;

    @JsonProperty("department_level")
    public Integer departmentLevel;

    @JsonProperty("department_name")
    public String departmentName;

    @JsonProperty("superior_department_id")
    public Long superiorDepartmentId;

    public DimDepartment() {
    }
}
