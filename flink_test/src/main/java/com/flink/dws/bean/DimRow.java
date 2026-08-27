package com.flink.dws.bean;

import java.io.Serializable;
import java.util.Map;

/** 维度广播行：统一封装 ods_industry / ods_employee / ods_department 的维度数据 */
public class DimRow implements Serializable {
    public String table;                // "industry" / "employee" / "department"
    public Long id;                     // 维度主键
    public Map<String, Object> data;    // 维度字段（after 整行）
    public DimRow() {}
}
