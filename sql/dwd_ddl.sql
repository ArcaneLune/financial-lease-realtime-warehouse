-- =====================================================
-- DWD 层 Doris 建表 SQL（事务事实表，DUPLICATE KEY + 按天范围分区 + 动态分区）
-- 在任意节点执行：mysql -h hadoop100 -P 9030 -uroot -proot
-- 说明：按天范围分区 + 动态分区自动管理生命周期；create_history_partition=true 创建历史分区
-- =====================================================

-- 1. 建库
CREATE DATABASE IF NOT EXISTS dwd;

use dwd;

-- 2. 新增授信事实表（授信域）
CREATE TABLE IF NOT EXISTS dwd.dwd_credit_add (
    id                   BIGINT          COMMENT '授信ID',
    add_time             DATETIMEV2      COMMENT '新增授信时间',
    credit_facility_id   BIGINT          COMMENT '授信申请ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复金额',
    credit_amount        DECIMAL(19,2)   COMMENT '正式授信金额'
)
DUPLICATE KEY(id, add_time)
COMMENT 'DWD层-授信域-新增授信事务事实表'
PARTITION BY RANGE(add_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 4. 授信占用事实表（授信域）
CREATE TABLE IF NOT EXISTS dwd.dwd_credit_occupy (
    id                   BIGINT          COMMENT '授信ID',
    occupy_time          DATETIMEV2      COMMENT '授信占用完成时间',
    credit_facility_id   BIGINT          COMMENT '授信申请ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复金额',
    credit_amount        DECIMAL(19,2)   COMMENT '正式授信金额'
)
DUPLICATE KEY(id, occupy_time)
COMMENT 'DWD层-授信域-授信占用事务事实表'
PARTITION BY RANGE(occupy_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 5. 审批通过事实表（审批域）
CREATE TABLE IF NOT EXISTS dwd.dwd_audit_approve (
    id                   BIGINT          COMMENT '授信申请ID',
    approve_time         DATETIMEV2      COMMENT '审批通过时间',
    lease_organization   VARCHAR(255)    COMMENT '业务方向',
    business_partner_id  BIGINT          COMMENT '申请人ID',
    industry_id          BIGINT          COMMENT '行业ID',
    reply_id             BIGINT          COMMENT '批复ID',
    salesman_id          BIGINT          COMMENT '业务经办ID',
    audit_man_id         BIGINT          COMMENT '信审经办ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请授信金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复授信金额',
    reply_time           DATETIMEV2      COMMENT '批复时间',
    irr                  DECIMAL(19,2)   COMMENT '还款利率',
    period               INT             COMMENT '还款期数'
)
DUPLICATE KEY(id, approve_time)
COMMENT 'DWD层-审批域-授信审批通过事务事实表'
PARTITION BY RANGE(approve_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 6. 审批取消事实表（审批域）
CREATE TABLE IF NOT EXISTS dwd.dwd_audit_cancel (
    id                   BIGINT          COMMENT '授信申请ID',
    cancel_time          DATETIMEV2      COMMENT '审批取消时间',
    lease_organization   VARCHAR(255)    COMMENT '业务方向',
    business_partner_id  BIGINT          COMMENT '申请人ID',
    industry_id          BIGINT          COMMENT '行业ID',
    salesman_id          BIGINT          COMMENT '业务经办ID',
    audit_man_id         BIGINT          COMMENT '信审经办ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请授信金额'
)
DUPLICATE KEY(id, cancel_time)
COMMENT 'DWD层-审批域-授信审批取消事务事实表'
PARTITION BY RANGE(cancel_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 7. 审批拒绝事实表（审批域）
CREATE TABLE IF NOT EXISTS dwd.dwd_audit_reject (
    id                   BIGINT          COMMENT '授信申请ID',
    reject_time          DATETIMEV2      COMMENT '审批拒绝时间',
    lease_organization   VARCHAR(255)    COMMENT '业务方向',
    business_partner_id  BIGINT          COMMENT '申请人ID',
    industry_id          BIGINT          COMMENT '行业ID',
    salesman_id          BIGINT          COMMENT '业务经办ID',
    audit_man_id         BIGINT          COMMENT '信审经办ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请授信金额'
)
DUPLICATE KEY(id, reject_time)
COMMENT 'DWD层-审批域-授信审批拒绝事务事实表'
PARTITION BY RANGE(reject_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 8. 合同制作事实表（租赁域）
CREATE TABLE IF NOT EXISTS dwd.dwd_lease_contract_produce (
    id                   BIGINT          COMMENT '合同ID',
    produced_time        DATETIMEV2      COMMENT '合同制作完成时间',
    credit_id            BIGINT          COMMENT '授信ID',
    credit_facility_id   BIGINT          COMMENT '授信申请ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复金额',
    credit_amount        DECIMAL(19,2)   COMMENT '正式授信金额'
)
DUPLICATE KEY(id, produced_time)
COMMENT 'DWD层-租赁域-合同制作事务事实表'
PARTITION BY RANGE(produced_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 9. 合同签约事实表（租赁域）
CREATE TABLE IF NOT EXISTS dwd.dwd_lease_sign (
    id                   BIGINT          COMMENT '合同ID',
    signed_time          DATETIMEV2      COMMENT '合同签约时间',
    credit_id            BIGINT          COMMENT '授信ID',
    credit_facility_id   BIGINT          COMMENT '授信申请ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复金额',
    credit_amount        DECIMAL(19,2)   COMMENT '正式授信金额'
)
DUPLICATE KEY(id, signed_time)
COMMENT 'DWD层-租赁域-合同签约事务事实表'
PARTITION BY RANGE(signed_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);

-- 10. 合同起租事实表（租赁域）
CREATE TABLE IF NOT EXISTS dwd.dwd_lease_execution (
    id                   BIGINT          COMMENT '合同ID',
    execution_time       DATETIMEV2      COMMENT '起租时间',
    credit_id            BIGINT          COMMENT '授信ID',
    credit_facility_id   BIGINT          COMMENT '授信申请ID',
    apply_amount         DECIMAL(19,2)   COMMENT '申请金额',
    reply_amount         DECIMAL(19,2)   COMMENT '批复金额',
    credit_amount        DECIMAL(19,2)   COMMENT '正式授信金额'
)
DUPLICATE KEY(id, execution_time)
COMMENT 'DWD层-租赁域-合同起租事务事实表'
PARTITION BY RANGE(execution_time) ()
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "compression" = "LZ4"
);
