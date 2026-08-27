package com.flink.dwd;

import java.io.Serializable;
import java.util.Map;

/** ODS 层统一封装：表名 + 操作类型 + 行数据（after）。用于单作业动态分流 */
public class OdsRecord implements Serializable {

    public String tableName;
    public String op;                // r=快照 c=插入 u=更新 d=删除
    public Map<String, Object> after; // 行数据（delete 时为 null）

    public OdsRecord() {
    }
}
