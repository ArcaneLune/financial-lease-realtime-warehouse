-- =====================================================
-- ADS 层异步物化视图（Doris 2.1）
-- 效果：从 DWD+DIM 聚合，Doris 每天自动刷新，FineBI 可直接查
-- 执行：mysql -h hadoop100 -P 9030 -uroot -proot < sql/ads_mv.sql
-- 说明：若某物化视图创建失败（Doris 版本对 UNION/JOIN 支持有限），退回 sql/ads_etl.sql（INSERT OVERWRITE）
-- =====================================================

CREATE DATABASE IF NOT EXISTS ads;

use ads;

-- 1. 综合统计：审批通过/取消/拒绝 累计
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_summary
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT 'approve' AS audit_result, count(*) AS project_count,
       sum(apply_amount) AS apply_amount, sum(reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve
UNION ALL
SELECT 'cancel', count(*), sum(apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel
UNION ALL
SELECT 'reject', count(*), sum(apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject;

-- 2. 各业务方向统计
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_org
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT lease_organization, 'approve' AS audit_result, count(*) AS project_count,
       sum(apply_amount) AS apply_amount, sum(reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve GROUP BY lease_organization
UNION ALL
SELECT lease_organization, 'cancel', count(*), sum(apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel GROUP BY lease_organization
UNION ALL
SELECT lease_organization, 'reject', count(*), sum(apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject GROUP BY lease_organization;

-- 3. 各部门统计（业务经办所属部门 3 级拍平）
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_dept
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT dep3.id AS department3_id, dep3.department_name AS department3_name,
       dep2.id AS department2_id, dep2.department_name AS department2_name,
       dep1.id AS department1_id, dep1.department_name AS department1_name,
       'approve' AS audit_result, count(*) AS project_count,
       sum(a.apply_amount) AS apply_amount, sum(a.reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve a
LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
LEFT JOIN dim.dim_department dep3 ON dep3.id = e.department_id
LEFT JOIN dim.dim_department dep2 ON dep2.id = dep3.superior_department_id
LEFT JOIN dim.dim_department dep1 ON dep1.id = dep2.superior_department_id
GROUP BY dep3.id, dep3.department_name, dep2.id, dep2.department_name, dep1.id, dep1.department_name
UNION ALL
SELECT dep3.id, dep3.department_name, dep2.id, dep2.department_name, dep1.id, dep1.department_name,
       'cancel', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel a
LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
LEFT JOIN dim.dim_department dep3 ON dep3.id = e.department_id
LEFT JOIN dim.dim_department dep2 ON dep2.id = dep3.superior_department_id
LEFT JOIN dim.dim_department dep1 ON dep1.id = dep2.superior_department_id
GROUP BY dep3.id, dep3.department_name, dep2.id, dep2.department_name, dep1.id, dep1.department_name
UNION ALL
SELECT dep3.id, dep3.department_name, dep2.id, dep2.department_name, dep1.id, dep1.department_name,
       'reject', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject a
LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
LEFT JOIN dim.dim_department dep3 ON dep3.id = e.department_id
LEFT JOIN dim.dim_department dep2 ON dep2.id = dep3.superior_department_id
LEFT JOIN dim.dim_department dep1 ON dep1.id = dep2.superior_department_id
GROUP BY dep3.id, dep3.department_name, dep2.id, dep2.department_name, dep1.id, dep1.department_name;

-- 4. 各业务经办统计
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_salesman
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT a.salesman_id, e.name AS salesman_name, 'approve' AS audit_result, count(*) AS project_count,
       sum(a.apply_amount) AS apply_amount, sum(a.reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve a LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
GROUP BY a.salesman_id, e.name
UNION ALL
SELECT a.salesman_id, e.name, 'cancel', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel a LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
GROUP BY a.salesman_id, e.name
UNION ALL
SELECT a.salesman_id, e.name, 'reject', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject a LEFT JOIN dim.dim_employee e ON e.id = a.salesman_id
GROUP BY a.salesman_id, e.name;

-- 5. 各信审经办统计
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_audit_man
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT a.audit_man_id, e.name AS audit_man_name, 'approve' AS audit_result, count(*) AS project_count,
       sum(a.apply_amount) AS apply_amount, sum(a.reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve a LEFT JOIN dim.dim_employee e ON e.id = a.audit_man_id
GROUP BY a.audit_man_id, e.name
UNION ALL
SELECT a.audit_man_id, e.name, 'cancel', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel a LEFT JOIN dim.dim_employee e ON e.id = a.audit_man_id
GROUP BY a.audit_man_id, e.name
UNION ALL
SELECT a.audit_man_id, e.name, 'reject', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject a LEFT JOIN dim.dim_employee e ON e.id = a.audit_man_id
GROUP BY a.audit_man_id, e.name;

-- 6. 各行业统计（行业 3 级拍平）
CREATE MATERIALIZED VIEW ads.mv_ads_audit_result_industry
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT ind3.id AS industry3_id, ind3.industry_name AS industry3_name,
       ind2.id AS industry2_id, ind2.industry_name AS industry2_name,
       ind1.id AS industry1_id, ind1.industry_name AS industry1_name,
       'approve' AS audit_result, count(*) AS project_count,
       sum(a.apply_amount) AS apply_amount, sum(a.reply_amount) AS reply_amount
FROM dwd.dwd_audit_approve a
LEFT JOIN dim.dim_industry ind3 ON ind3.id = a.industry_id
LEFT JOIN dim.dim_industry ind2 ON ind2.id = ind3.superior_industry_id
LEFT JOIN dim.dim_industry ind1 ON ind1.id = ind2.superior_industry_id
GROUP BY ind3.id, ind3.industry_name, ind2.id, ind2.industry_name, ind1.id, ind1.industry_name
UNION ALL
SELECT ind3.id, ind3.industry_name, ind2.id, ind2.industry_name, ind1.id, ind1.industry_name,
       'cancel', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_cancel a
LEFT JOIN dim.dim_industry ind3 ON ind3.id = a.industry_id
LEFT JOIN dim.dim_industry ind2 ON ind2.id = ind3.superior_industry_id
LEFT JOIN dim.dim_industry ind1 ON ind1.id = ind2.superior_industry_id
GROUP BY ind3.id, ind3.industry_name, ind2.id, ind2.industry_name, ind1.id, ind1.industry_name
UNION ALL
SELECT ind3.id, ind3.industry_name, ind2.id, ind2.industry_name, ind1.id, ind1.industry_name,
       'reject', count(*), sum(a.apply_amount), CAST(NULL AS DECIMAL(19,2))
FROM dwd.dwd_audit_reject a
LEFT JOIN dim.dim_industry ind3 ON ind3.id = a.industry_id
LEFT JOIN dim.dim_industry ind2 ON ind2.id = ind3.superior_industry_id
LEFT JOIN dim.dim_industry ind1 ON ind1.id = ind2.superior_industry_id
GROUP BY ind3.id, ind3.industry_name, ind2.id, ind2.industry_name, ind1.id, ind1.industry_name;

-- 7. 已审项目转化主题（授信漏斗 5 环节累计）
CREATE MATERIALIZED VIEW ads.mv_ads_credit_transform
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES ('replication_num' = '2')
AS
SELECT 'credit_add' AS transform_step, count(*) AS project_count,
       sum(apply_amount) AS apply_amount, sum(reply_amount) AS reply_amount, sum(credit_amount) AS credit_amount
FROM dwd.dwd_credit_add
UNION ALL
SELECT 'credit_occupy', count(*), sum(apply_amount), sum(reply_amount), sum(credit_amount)
FROM dwd.dwd_credit_occupy
UNION ALL
SELECT 'contract_produce', count(*), sum(apply_amount), sum(reply_amount), sum(credit_amount)
FROM dwd.dwd_lease_contract_produce
UNION ALL
SELECT 'lease_sign', count(*), sum(apply_amount), sum(reply_amount), sum(credit_amount)
FROM dwd.dwd_lease_sign
UNION ALL
SELECT 'lease_execution', count(*), sum(apply_amount), sum(reply_amount), sum(credit_amount)
FROM dwd.dwd_lease_execution;
