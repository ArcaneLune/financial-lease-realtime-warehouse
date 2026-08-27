# 金融租赁实时数仓（Financial Lease Realtime Warehouse）

基于 **Flink CDC + Kafka + Doris** 构建的**金融租赁授信审批业务实时数仓**，五层建模（ODS / DIM / DWD / DWS / ADS），**FineBI** 可视化。

## 📌 项目简介

本项目针对金融租赁「授信审批」业务，从 MySQL 业务库采集 binlog，构建一条**端到端的实时数仓链路**，实现审批进度、授信转化、各维度业绩的实时/近实时监控与分析。

```
MySQL(金融租赁业务库 financial_lease)
   │  Flink CDC 采集 binlog（DataStream API）
   ▼
Kafka（ODS 层，动态路由：每表一个 topic，命名 ods_表名）
   │  Flink 读取 Kafka（DataStream API）
   ▼
Doris（数仓建模：DIM / DWD / DWS / ADS 四层）
   │
   ▼
FineBI（可视化大屏/报表）
```

## 🛠 技术栈

| 组件 | 版本 | 说明 |
|---|---|---|
| Flink CDC | 2.4.2 | 采集 MySQL binlog（配 Flink 1.17） |
| Flink | 1.17.1 | DataStream API，standalone 模式 |
| Kafka | 2.12-3.3.1 | ODS 层消息队列 |
| Doris | 2.1.0 | OLAP 分析库（DIM/DWD/DWS/ADS） |
| FineBI | - | 可视化报表/大屏 |

## 📂 目录结构

```
金融实时数仓项目/
├── flink_test/ # IDEA 数仓建模代码工程（Flink DataStream API，五层实现）
├── sql/        # 数仓各层建表语句
│   ├── dim_ddl.sql     # DIM 层（4 张维度表）
│   ├── dwd_ddl.sql     # DWD 层（8 张事实表）
│   ├── dws_ddl.sql     # DWS 层（11 张窗口聚合表）
│   └── ads_mv.sql      # ADS 层（7 张物化视图）
├── scripts/    # Linux 脚本（Kafka topic、启停等）
├── doc/        # 从 0-1 搭建教程文档
│   ├── 实时前置环境搭建文档.md
│   ├── 数仓建模流程文档.md
│   ├── 金融租赁数仓建模开发过程.md
│   └── FineBI可视化制作文档.md
└── images/     # 项目过程截图
```

## 💻 代码工程（flink_test）

Flink DataStream API 实现的五层数仓代码（Maven 工程，Java 8），核心类如下：

```
com.flink
├── cdc/   MysqlCdcToKafka          # ODS：Flink CDC 整库同步 + 按 source.table 动态路由到 ods_表名
├── dim/   DimUtil                   # 通用工具：KafkaSource/DorisSink/时间/金额解析
│          Dim*Job（4 个）            # DIM：部门/员工/行业/商业合伙人维度表
├── dwd/   DwdBaseJob                # DWD：单作业动态分流（读 5 表 → union → keyBy → 侧输出）
│          DwdProcessFunction       # 核心：状态机事件识别 + 谁先到谁等（KeyedProcessFunction + ValueState）
│          LeaseSignProcessFunction # 租赁域双流关联（合同制作 ⋈ 签约/起租）
│          OdsRecord / pojo/*       # 统一记录 / 事实表 POJO（含 @JsonProperty 对齐 Doris 列）
└── dws/   DwsDimUtil                # 维度广播：从 ODS topic 读维度表广播，供 DWS 拍平
           Dws*Win（11 个）           # DWS：10 秒窗口聚合 + 维度 3 级拍平（无维度 5 张 + 带维度 6 张）
```

**代码特点**：
- ODS/DIM/DWD 用 DataStream API，DWD 采用「单作业动态分流」架构（借鉴尚硅谷 DwdBaseApp 并适配 Doris）。
- DWD 输出统一为 String(JSON)，规避 Flink 异构 POJO 侧输出类型传播 bug。
- DWS 用事件时间 + 10 秒滚动窗口，维度表用 BroadcastState 广播拍平（替代 HBase 异步 I/O）。
- 全量代码含详细注释（事件规则、状态机、设计决策）。

**运行方式**（详见 `doc/数仓建模流程文档.md`）：
```bash
mvn clean package -DskipTests   # 打 fat jar
bin/flink run -c com.flink.cdc.MysqlCdcToKafka  jar/flink_test-1.0-SNAPSHOT.jar  # 按层逐个提交
```



| 层 | 存储 | 内容 | 说明 |
|----|------|------|------|
| ODS | Kafka | 9 张表 CDC 原始数据 | Flink CDC 单作业整库读，debezium-json，动态路由 |
| DIM | Doris | 4 张维度表 | 部门/员工/行业/商业合伙人，raw mirror |
| DWD | Doris + Kafka | 8 张事务事实表 | 审批/授信/租赁三域，单作业动态分流，双写 |
| DWS | Doris | 11 张窗口聚合表 | 10 秒滚动窗口，AGGREGATE KEY + REPLACE，维度拍平 |
| ADS | Doris | 7 张物化视图 | 已审项目 6 粒度 + 转化漏斗，5 分钟自动刷新 |

### 业务链路（状态机）

授信申请 → 多级审批（风控 → 信审 → 一级/二级评审 → 项目评审会 → 总经理）→ 出具批复（status=16）→ 新增授信 → 授信占用 → 合同制作 → 签约 → 起租。

## ✅ 数据验证

- **DWD 漏斗自洽**：新增授信 1272 → 授信占用 1035 → 合同制作 940 → 签约 884 → 起租 702（层层递减）
- **ADS 一致性**：6 张粒度表 project_count / 金额 总和 = DWD 行数，完全一致
- **维度一致性**：DWS/ADS 维度名称与 dim 表 join 无差异

## 📊 可视化（FineBI）

- 综合统计 KPI 卡片（审批通过/取消/拒绝）
- 授信转化漏斗图（1272→702 转化流失）
- 业务方向饼图、行业/部门/经办多维柱状图

## 🚀 快速开始

从 0-1 搭建步骤见 `doc/` 目录：
1. `实时前置环境搭建文档.md` — 集群环境（MySQL/Kafka/Flink/Doris）搭建
2. `数仓建模流程文档.md` — 五层建模步骤与命令
3. `金融租赁数仓建模开发过程.md` — 按业务总线矩阵 + 指标体系的设计决策与业务思考
4. `FineBI可视化制作文档.md` — Doris 数据接入与报表制作

## ⚠️ 测试环境说明

本项目为**学习/求职项目**，代码中的 IP（hadoop100/101/102）、账号（root/root）均为**测试环境配置**，生产环境请替换为实际地址与凭据。

---
*2026 年 8 月 · 数据开发求职项目*
