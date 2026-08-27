-- =====================================================
-- DWS 层 Doris 建表 SQL（11 张汇总表，AGGREGATE KEY + 指标 REPLACE）
-- 在任意节点执行：mysql -h hadoop100 -P 9030 -uroot -proot
-- 说明：Flink 端做窗口聚合后写入，Doris 端用 REPLACE 保证幂等（同一窗口重发覆盖而非累加）
-- =====================================================

CREATE DATABASE IF NOT EXISTS dws;

use dws;

-- ===================== 审批域：行业 + 业务方向 + 业务经办粒度 =====================

-- 1. 审批通过
CREATE TABLE IF NOT EXISTS dws.dws_audit_industry_lease_organization_salesman_approval_win (
    `stt`                DATETIME       COMMENT '窗口起始时间',
    `edt`                DATETIME       COMMENT '窗口结束时间',
    `cur_date`           DATE           COMMENT '当天日期',
    `industry1_id`       BIGINT         COMMENT '一级行业ID',
    `industry1_name`     VARCHAR(255)   COMMENT '一级行业名称',
    `industry2_id`       BIGINT         COMMENT '二级行业ID',
    `industry2_name`     VARCHAR(255)   COMMENT '二级行业名称',
    `industry3_id`       BIGINT         COMMENT '三级行业ID',
    `industry3_name`     VARCHAR(255)   COMMENT '三级行业名称',
    `lease_organization` VARCHAR(255)   COMMENT '业务方向',
    `salesman_id`        BIGINT         COMMENT '业务经办ID',
    `salesman_name`      VARCHAR(255)   COMMENT '业务经办姓名',
    `department1_id`     BIGINT         COMMENT '一级部门ID',
    `department1_name`   VARCHAR(255)   COMMENT '一级部门名称',
    `department2_id`     BIGINT         COMMENT '二级部门ID',
    `department2_name`   VARCHAR(255)   COMMENT '二级部门名称',
    `department3_id`     BIGINT         COMMENT '三级部门ID',
    `department3_name`   VARCHAR(255)   COMMENT '三级部门名称',
    `apply_count`        BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`       DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`       DECIMAL(19,2) REPLACE COMMENT '批复金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
COMMENT 'DWS层-审批域-行业业务方向业务经办粒度-审批通过窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 2. 审批取消（指标：apply_count + apply_amount，无 reply_amount）
CREATE TABLE IF NOT EXISTS dws.dws_audit_industry_lease_organization_salesman_cancel_win (
    `stt`                DATETIME       COMMENT '窗口起始时间',
    `edt`                DATETIME       COMMENT '窗口结束时间',
    `cur_date`           DATE           COMMENT '当天日期',
    `industry1_id`       BIGINT         COMMENT '一级行业ID',
    `industry1_name`     VARCHAR(255)   COMMENT '一级行业名称',
    `industry2_id`       BIGINT         COMMENT '二级行业ID',
    `industry2_name`     VARCHAR(255)   COMMENT '二级行业名称',
    `industry3_id`       BIGINT         COMMENT '三级行业ID',
    `industry3_name`     VARCHAR(255)   COMMENT '三级行业名称',
    `lease_organization` VARCHAR(255)   COMMENT '业务方向',
    `salesman_id`        BIGINT         COMMENT '业务经办ID',
    `salesman_name`      VARCHAR(255)   COMMENT '业务经办姓名',
    `department1_id`     BIGINT         COMMENT '一级部门ID',
    `department1_name`   VARCHAR(255)   COMMENT '一级部门名称',
    `department2_id`     BIGINT         COMMENT '二级部门ID',
    `department2_name`   VARCHAR(255)   COMMENT '二级部门名称',
    `department3_id`     BIGINT         COMMENT '三级部门ID',
    `department3_name`   VARCHAR(255)   COMMENT '三级部门名称',
    `apply_count`        BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`       DECIMAL(19,2) REPLACE COMMENT '申请金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
COMMENT 'DWS层-审批域-行业业务方向业务经办粒度-审批取消窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 3. 审批拒绝（指标：apply_count + apply_amount）
CREATE TABLE IF NOT EXISTS dws.dws_audit_industry_lease_organization_salesman_reject_win (
    `stt`                DATETIME       COMMENT '窗口起始时间',
    `edt`                DATETIME       COMMENT '窗口结束时间',
    `cur_date`           DATE           COMMENT '当天日期',
    `industry1_id`       BIGINT         COMMENT '一级行业ID',
    `industry1_name`     VARCHAR(255)   COMMENT '一级行业名称',
    `industry2_id`       BIGINT         COMMENT '二级行业ID',
    `industry2_name`     VARCHAR(255)   COMMENT '二级行业名称',
    `industry3_id`       BIGINT         COMMENT '三级行业ID',
    `industry3_name`     VARCHAR(255)   COMMENT '三级行业名称',
    `lease_organization` VARCHAR(255)   COMMENT '业务方向',
    `salesman_id`        BIGINT         COMMENT '业务经办ID',
    `salesman_name`      VARCHAR(255)   COMMENT '业务经办姓名',
    `department1_id`     BIGINT         COMMENT '一级部门ID',
    `department1_name`   VARCHAR(255)   COMMENT '一级部门名称',
    `department2_id`     BIGINT         COMMENT '二级部门ID',
    `department2_name`   VARCHAR(255)   COMMENT '二级部门名称',
    `department3_id`     BIGINT         COMMENT '三级部门ID',
    `department3_name`   VARCHAR(255)   COMMENT '三级部门名称',
    `apply_count`        BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`       DECIMAL(19,2) REPLACE COMMENT '申请金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`industry1_id`,`industry1_name`,`industry2_id`,`industry2_name`,`industry3_id`,`industry3_name`,`lease_organization`,`salesman_id`,`salesman_name`,`department1_id`,`department1_name`,`department2_id`,`department2_name`,`department3_id`,`department3_name`)
COMMENT 'DWS层-审批域-行业业务方向业务经办粒度-审批拒绝窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- ===================== 审批域：信审经办粒度 =====================

-- 4. 信审经办-审批通过
CREATE TABLE IF NOT EXISTS dws.dws_audit_audit_man_approval_win (
    `stt`            DATETIME       COMMENT '窗口起始时间',
    `edt`            DATETIME       COMMENT '窗口结束时间',
    `cur_date`       DATE           COMMENT '当天日期',
    `audit_man_id`   BIGINT         COMMENT '信审经办ID',
    `audit_man_name` VARCHAR(255)   COMMENT '信审经办姓名',
    `apply_count`    BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`   DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`   DECIMAL(19,2) REPLACE COMMENT '批复金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
COMMENT 'DWS层-审批域-信审经办粒度-审批通过窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 5. 信审经办-审批取消
CREATE TABLE IF NOT EXISTS dws.dws_audit_audit_man_cancel_win (
    `stt`            DATETIME       COMMENT '窗口起始时间',
    `edt`            DATETIME       COMMENT '窗口结束时间',
    `cur_date`       DATE           COMMENT '当天日期',
    `audit_man_id`   BIGINT         COMMENT '信审经办ID',
    `audit_man_name` VARCHAR(255)   COMMENT '信审经办姓名',
    `apply_count`    BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`   DECIMAL(19,2) REPLACE COMMENT '申请金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
COMMENT 'DWS层-审批域-信审经办粒度-审批取消窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 6. 信审经办-审批拒绝
CREATE TABLE IF NOT EXISTS dws.dws_audit_audit_man_reject_win (
    `stt`            DATETIME       COMMENT '窗口起始时间',
    `edt`            DATETIME       COMMENT '窗口结束时间',
    `cur_date`       DATE           COMMENT '当天日期',
    `audit_man_id`   BIGINT         COMMENT '信审经办ID',
    `audit_man_name` VARCHAR(255)   COMMENT '信审经办姓名',
    `apply_count`    BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`   DECIMAL(19,2) REPLACE COMMENT '申请金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`,`audit_man_id`,`audit_man_name`)
COMMENT 'DWS层-审批域-信审经办粒度-审批拒绝窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- ===================== 授信域（无维度，纯时间窗口） =====================

-- 7. 新增授信
CREATE TABLE IF NOT EXISTS dws.dws_credit_credit_add_win (
    `stt`           DATETIME       COMMENT '窗口起始时间',
    `edt`           DATETIME       COMMENT '窗口结束时间',
    `cur_date`      DATE           COMMENT '当天日期',
    `apply_count`   BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`  DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`  DECIMAL(19,2) REPLACE COMMENT '批复金额',
    `credit_amount` DECIMAL(19,2) REPLACE COMMENT '授信金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`)
COMMENT 'DWS层-授信域-新增授信窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 8. 授信占用
CREATE TABLE IF NOT EXISTS dws.dws_credit_credit_occupy_win (
    `stt`           DATETIME       COMMENT '窗口起始时间',
    `edt`           DATETIME       COMMENT '窗口结束时间',
    `cur_date`      DATE           COMMENT '当天日期',
    `apply_count`   BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`  DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`  DECIMAL(19,2) REPLACE COMMENT '批复金额',
    `credit_amount` DECIMAL(19,2) REPLACE COMMENT '授信金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`)
COMMENT 'DWS层-授信域-授信占用窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- ===================== 租赁域（无维度，纯时间窗口） =====================

-- 9. 合同制作
CREATE TABLE IF NOT EXISTS dws.dws_lease_contract_produce_win (
    `stt`           DATETIME       COMMENT '窗口起始时间',
    `edt`           DATETIME       COMMENT '窗口结束时间',
    `cur_date`      DATE           COMMENT '当天日期',
    `apply_count`   BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`  DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`  DECIMAL(19,2) REPLACE COMMENT '批复金额',
    `credit_amount` DECIMAL(19,2) REPLACE COMMENT '授信金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`)
COMMENT 'DWS层-租赁域-合同制作窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 10. 签约
CREATE TABLE IF NOT EXISTS dws.dws_lease_sign_win (
    `stt`           DATETIME       COMMENT '窗口起始时间',
    `edt`           DATETIME       COMMENT '窗口结束时间',
    `cur_date`      DATE           COMMENT '当天日期',
    `apply_count`   BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`  DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`  DECIMAL(19,2) REPLACE COMMENT '批复金额',
    `credit_amount` DECIMAL(19,2) REPLACE COMMENT '授信金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`)
COMMENT 'DWS层-租赁域-签约窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);

-- 11. 起租
CREATE TABLE IF NOT EXISTS dws.dws_lease_execution_win (
    `stt`           DATETIME       COMMENT '窗口起始时间',
    `edt`           DATETIME       COMMENT '窗口结束时间',
    `cur_date`      DATE           COMMENT '当天日期',
    `apply_count`   BIGINT REPLACE COMMENT '申请项目数',
    `apply_amount`  DECIMAL(19,2) REPLACE COMMENT '申请金额',
    `reply_amount`  DECIMAL(19,2) REPLACE COMMENT '批复金额',
    `credit_amount` DECIMAL(19,2) REPLACE COMMENT '授信金额'
)
AGGREGATE KEY (`stt`,`edt`,`cur_date`)
COMMENT 'DWS层-租赁域-起租窗口汇总'
PARTITION BY RANGE(`cur_date`) ()
DISTRIBUTED BY HASH(`stt`) BUCKETS 10
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-100",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "100"
);
