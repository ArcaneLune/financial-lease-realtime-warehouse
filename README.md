# 金融租赁实时数仓（Financial Lease Realtime Warehouse）

> 基于 Flink CDC、Kafka、Flink DataStream 和 Apache Doris 搭建的金融租赁授信审批实时数仓。项目围绕授信申请、审批、授信占用、合同签约和起租等业务环节，完成 ODS、DIM、DWD、DWS、ADS 五层建模，并使用 FineBI 制作经营分析驾驶舱。

[![Flink CDC](https://img.shields.io/badge/Flink%20CDC-2.4.2-blue)](https://ververica.github.io/flink-cdc-connectors/)
[![Flink](https://img.shields.io/badge/Flink-1.17.1-purple)](https://flink.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.3.1-red)](https://kafka.apache.org/)
[![Doris](https://img.shields.io/badge/Doris-2.1.0-green)](https://doris.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 项目概述

在 3 节点 CentOS 7 虚拟机集群上，针对金融租赁授信审批业务搭建了一套 Kappa 风格实时数仓。数据从 MySQL 业务库产生后，经 Flink CDC 采集到 Kafka，再由 Flink 完成实时分层和指标计算，最终写入 Doris，供 FineBI 查询和展示。

- **数据采集**：Flink CDC 单作业读取 `financial_lease` 全部 9 张表，使用 debezium-json，并根据 `source.table` 动态路由到 `ods_表名` Kafka Topic。
- **数仓建模**：使用 Flink DataStream API 完成 ODS → DIM → DWD → DWS → ADS 五层处理。
  - DIM：4 张维度表（部门、员工、行业、商业合伙人）。
  - DWD：8 张事务事实表，覆盖审批通过/拒绝/取消、新增授信/授信占用、合同制作/签约/起租。
  - DWS：11 张 10 秒滚动窗口聚合表，完成维度拍平和指标汇总。
  - ADS：7 张 Doris 异步物化视图，包含已审项目 6 种粒度和授信转化漏斗。
- **数据服务**：FineBI 连接 Doris `ads` 库，展示审批进度、授信金额、转化漏斗和多维经营指标。

## 项目背景与价值

### 为什么要做这个项目？

金融租赁业务中，客户提交授信申请后，需要经过风控初审、信审、一级/二级评审、项目评审会和总经理审批，之后还要经历授信占用、合同制作、签约和起租。业务链路长、状态变化多，管理人员希望实时了解项目进度、审批金额以及每个环节的转化情况。

传统 T+1 报表只能在第二天提供结果，无法满足审批过程监控和风险管理的需要。为了解决这个问题，选择以 Kafka 作为统一消息入口，以 Flink 进行实时计算，以 Doris 作为实时分析存储，搭建从业务库到驾驶舱的实时数据 Pipeline。

### 解决了什么问题

| 业务问题 | 处理方式 |
|---|---|
| 审批进度滞后，无法及时定位卡点 | 将审批流水实时采集并识别当前业务环节 |
| 授信转化漏斗依赖日报或月报 | 通过 DWD 事实事件和 DWS 窗口聚合实时计算申请→批复→占用→签约→起租 |
| 维度信息更新不及时 | 部门、员工、行业和商业合伙人进入实时 DIM，并参与事实关联 |
| 多表状态难以统一分析 | 以 `credit_facility_id` 维护业务上下文，统一生成8类业务事实 |
| BI 直接查询明细表成本高 | 使用 Doris DWS 和 ADS 物化视图提供面向分析的结果表 |

## 业务逻辑说明

### 业务场景

将 `credit_facility` 作为授信申请主线，并结合状态流水、授信表和合同表还原完整生命周期。项目重点处理的是“状态变化产生的业务事件”，而不是简单同步业务表的当前值。

### 授信生命周期（状态机）

![信贷金融业务流程](doc/信贷金融业务流程.png)

```text
授信申请 → 多级审批 → 出具批复（status=16） → 新增授信 → 授信占用
                                      → 合同制作 → 签约 → 起租
                       ├── 拒绝（status=20）
                       └── 取消（status=21）
```

| 环节 | 业务含义 | 数据来源 |
|---|---|---|
| 授信申请 | 客户提交授信申请 | `credit_facility` 插入 |
| 多级审批 | 风控、信审、评审会和总经理逐级审核 | `credit_facility_status` 流转记录 |
| 审批通过 | 出具授信批复 | 流水表 `status = 16` |
| 审批拒绝 | 风控或信审拒绝申请 | 流水表 `status = 20` |
| 审批取消 | 客户或业务人员取消申请 | 流水表 `status = 21` |
| 新增授信 | 批复后生成授信额度 | `credit` 插入 |
| 授信占用 | 客户实际支用额度 | `credit.occupy_time` 非空 |
| 合同制作 | 生成租赁合同 | `contract.produce_time` 非空 |
| 签约 | 双方签署合同 | `contract.signed_time` 非空 |
| 起租 | 合同生效并开始计租 | `contract.execution_time` 非空 |

开发过程中我确认了一个重要口径：审批结果要看状态流水，授信占用和合同进度要看时间字段，不能只读取主表的当前 `status`。例如，审批通过后状态还会继续流转到新增授信，如果只看最终状态就会漏掉审批通过事件。

### 核心业务指标

| 指标 | 口径 | 所在层 |
|---|---|---|
| 已审项目数 | 审批通过、拒绝、取消的授信申请数 | ADS |
| 审批金额 | 申请金额、批复金额 | DWD / ADS |
| 授信转化漏斗 | 新增授信→占用→制作→签约→起租各环节项目数 | ADS |
| 实时申请/批复 | 10秒窗口内的笔数和金额 | DWS |
| 维度画像 | 行业、部门、业务经办和信审经办粒度分布 | DWS / ADS |

## 项目技术架构

![项目架构图](images/项目架构图.jpeg)

```text
MySQL（金租业务库 financial_lease）
      │ Flink CDC 整库采集
      ▼
Kafka ODS（debezium-json，按表动态路由）
      │ Flink DataStream
      ├── DIM：实时维度镜像
      ├── DWD：业务事实识别，双写 Doris + Kafka
      └── DWS：10秒窗口聚合
                    │
                    ▼
             Doris ADS 物化视图
                    │
                    ▼
                 FineBI 大屏
```

在 DWD 层使用 keyed state 保存申请上下文，通过侧输出拆分不同业务过程，并使用广播维度完成名称关联。DWS 层按业务主题拆成多个独立作业，ADS 层通过 Doris 异步物化视图提供查询结果。

## 项目截图

| 集群进程（jps） | Kafka ODS 数据 |
|:---:|:---:|
| ![jps](images/jps-processes.png) | ![Kafka](images/kafka-ods.png) |

| Doris DWD 明细 | DWS 窗口聚合 |
|:---:|:---:|
| ![DWD](images/doris-dwd.png) | ![DWS](images/doris-dws.png) |

| Doris ADS 物化视图 | FineBI 大屏 |
|:---:|:---:|
| ![ADS](images/doris-ads.png) | ![FineBI](images/finebi-dashboard.png) |

| Doris FE Web UI | Doris 各层表（DataGrip） |
|:---:|:---:|
| ![Doris FE](images/doris-fe-web.png) | ![DataGrip](images/doris-datagrip-tables.png) |

## 技术栈

| 类别 | 组件 | 版本 |
|---|---|---|
| 业务数据库 | MySQL | 8.0.39 |
| 数据采集 | Flink CDC | 2.4.2 |
| 消息队列 | Kafka | 2.12-3.3.1 |
| 流计算引擎 | Flink | 1.17.1 |
| OLAP 引擎 | Apache Doris | 2.1.0 |
| 协调服务 | ZooKeeper | 3.7.1 |
| 数据可视化 | FineBI | - |

## 集群规划

| 服务 | hadoop100 | hadoop101 | hadoop102 |
|---|:---:|:---:|:---:|
| MySQL | ✓ | | |
| ZooKeeper | ✓ | ✓ | ✓ |
| Kafka | ✓ | ✓ | ✓ |
| Flink | JM + TM | TM | TM |
| Doris FE | ✓ | | |
| Doris BE | | ✓ | ✓ |
| 模拟数据 | ✓ | | |

> IP：hadoop100=192.168.100.130，hadoop101=192.168.100.131，hadoop102=192.168.100.132。

## 数仓分层

| 层 | 存储 | 内容 | 说明 |
|---|---|---|---|
| ODS | Kafka | 9张表 CDC 原始数据 | 单作业整库读取，按表动态路由 |
| DIM | Doris | 4张维度表 | 部门、员工、行业、商业合伙人 |
| DWD | Doris + Kafka | 8张事务事实表 | 审批、授信、租赁三域，单作业动态分流 |
| DWS | Doris | 11张窗口聚合表 | 10秒滚动窗口，维度拍平 |
| ADS | Doris | 7张物化视图 | 已审项目6种粒度和转化漏斗 |

### 业务链路（状态机）

授信申请 → 多级审批（风控 → 信审 → 一级/二级评审 → 项目评审会 → 总经理）→ 出具批复（status=16）→ 新增授信 → 授信占用 → 合同制作 → 签约 → 起租。

## 目录结构

```text
financial-lease-realtime-warehouse/
├── README.md                 # 项目说明、架构和运行入口
├── 项目介绍.md                # 业务理解、指标体系和设计过程
├── flink_test/               # Flink CDC、DIM、DWD、DWS 源码工程
├── sql/                      # 各层 DDL 和 ADS 物化视图 SQL
│   ├── dim_ddl.sql           # DIM 层4张维度表
│   ├── dwd_ddl.sql           # DWD 层8张事实表
│   ├── dws_ddl.sql           # DWS 层11张窗口表
│   └── ads_mv.sql            # ADS 层7张物化视图
├── doc/                      # 环境搭建、建模开发、FineBI 制作文档
│   ├── 00-业务理解与指标处理.md
│   ├── 01-实时环境搭建.md
│   ├── 02-数仓建模开发.md
│   ├── 03-FineBI可视化制作.md
│   ├── 业务总线矩阵.xlsx
│   ├── 信贷金融实时指标体系.xlsx
│   └── 事实表.txt
├── scripts/                  # 集群启停和文件分发脚本
│   ├── zk.sh / kf.sh / hdp.sh
│   ├── xsync / xcall
│   └── doris-cluster.sh
├── images/                   # 架构图和项目截图
└── 模拟数据脚本/              # 业务模拟数据生成器
```

## 快速开始

### 1. 启动集群

```bash
sh zk.sh start
sh kf.sh start
sh doris-cluster.sh start
bin/start-cluster.sh
```

### 2. 建库建表

```bash
mysql -h hadoop100 -P 9030 -uroot -proot < sql/dim_ddl.sql
mysql -h hadoop100 -P 9030 -uroot -proot < sql/dwd_ddl.sql
mysql -h hadoop100 -P 9030 -uroot -proot < sql/dws_ddl.sql
mysql -h hadoop100 -P 9030 -uroot -proot < sql/ads_mv.sql
```

### 3. 生成模拟数据 + 启动 ODS 采集

```bash
java -jar 模拟数据脚本/mock-finance-1.3.0.jar
bin/flink run -c com.flink.cdc.MysqlCdcToKafka jar/flink_test-1.0-SNAPSHOT.jar
```

### 4. 提交 Flink 数仓作业（按层）

```bash
bin/flink run -c com.flink.dim.DimDepartmentJob -d jar/flink_test-1.0-SNAPSHOT.jar
bin/flink run -c com.flink.dwd.DwdBaseJob -d jar/flink_test-1.0-SNAPSHOT.jar
bin/flink run -c com.flink.dws.DwsCreditCreditAddWin -d jar/flink_test-1.0-SNAPSHOT.jar
```

其余 DIM、DWS 作业以及 FineBI 配置见 `doc/02-数仓建模开发.md` 和 `doc/03-FineBI可视化制作.md`。

### 5. ADS 物化视图 + FineBI

```bash
mysql -h hadoop100 -P 9030 -uroot -proot < sql/ads_mv.sql
```

FineBI 连接 Doris `ads` 库后，导入 `mv_ads_*` 视图制作经营分析报表。

### Web 控制台

| 组件 | 地址 |
|---|---|
| Flink Web UI | http://hadoop100:8081 |
| Doris FE | http://hadoop100:8030 |
| FineBI | 本地客户端 |

## 数据验证

- **DWD 漏斗自洽**：新增授信1272 → 授信占用1035 → 合同制作940 → 签约884 → 起租702。
- **ADS 一致性**：6张粒度表的项目数和金额汇总与 DWD 明细核对一致。
- **维度一致性**：DWS/ADS 中的维度名称与 DIM 表关联结果一致。

## 关键指标

| 指标 | 数值 |
|---|---:|
| 数据表总数 | ODS 9 + DIM 4 + DWD 8 + DWS 11 + ADS 7 = 39 |
| DWD 事实表 | 8张，覆盖审批/授信/租赁三域 |
| DWS 汇总表 | 11张，10秒滚动窗口 |
| ADS 物化视图 | 7张，5分钟自动刷新 |
| 授信转化漏斗 | 1272 → 1035 → 940 → 884 → 702 |
| 审批终态 | 通过1328 + 拒绝1088 + 取消269 |
| 集群规模 | 3节点 CentOS 7.9 |

## License

MIT
