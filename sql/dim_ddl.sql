-- =====================================================
-- DIM 层 Doris 建表 SQL（原始镜像结构，类型对齐 MySQL）
-- 在任意节点执行：mysql -h hadoop100 -P 9030 -uroot -proot
-- =====================================================

-- 1. 建库
CREATE DATABASE IF NOT EXISTS dim;

use dim;

-- 2. 商业合伙人（客户）维度表
CREATE TABLE IF NOT EXISTS dim.dim_business_partner (
    id          BIGINT       COMMENT '合伙人/客户主键ID',
    create_time DATETIMEV2   COMMENT '记录创建时间',
    update_time DATETIMEV2   COMMENT '记录更新时间',
    name        VARCHAR(256) COMMENT '合伙人/客户名称'
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '2');

-- 3. 部门维度表
CREATE TABLE IF NOT EXISTS dim.dim_department (
    id                     BIGINT       COMMENT '部门主键ID',
    create_time            DATETIMEV2   COMMENT '记录创建时间',
    update_time            DATETIMEV2   COMMENT '记录更新时间',
    department_level       INT          COMMENT '部门级别',
    department_name        VARCHAR(128) COMMENT '部门名称',
    superior_department_id BIGINT       COMMENT '上级部门ID'
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '2');

-- 4. 员工维度表
CREATE TABLE IF NOT EXISTS dim.dim_employee (
    id            BIGINT       COMMENT '员工主键ID',
    create_time   DATETIMEV2   COMMENT '记录创建时间',
    update_time   DATETIMEV2   COMMENT '记录更新时间',
    name          VARCHAR(64)  COMMENT '员工姓名',
    type          BIGINT       COMMENT '员工类型/岗位',
    department_id BIGINT       COMMENT '所属部门ID'
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '2');

-- 5. 行业维度表
CREATE TABLE IF NOT EXISTS dim.dim_industry (
    id                   BIGINT       COMMENT '行业主键ID',
    create_time          DATETIMEV2   COMMENT '记录创建时间',
    update_time          DATETIMEV2   COMMENT '记录更新时间',
    industry_level       INT          COMMENT '行业级别',
    industry_name        VARCHAR(128) COMMENT '行业名称',
    superior_industry_id BIGINT       COMMENT '上级行业ID'
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '2');
