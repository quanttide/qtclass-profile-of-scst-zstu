# 数据湖架构

以下是现代数据湖架构的核心组成部分与技术特点的科普解析，以分层逻辑呈现其基本结构：

1. 存储层（核心基础）

• 对象存储主导：
使用云原生对象存储（如AWS S3、Azure Data Lake Storage、阿里云OSS）或分布式文件系统（如HDFS），以低成本、高扩展性存储原始格式数据（JSON、Parquet、CSV、图片、日志等）。

• 分层存储策略：

  ◦ 原始数据区：未经处理的原始数据（Raw Zone）

  ◦ 清洗/转换区：ETL后的结构化数据（Curated Zone）

  ◦ 服务区：面向应用的高性能存储（如Delta Lake、Iceberg表）

2. 元数据层（数据可发现性）

• 统一元数据管理：
通过元数据目录（如AWS Glue、Hive Metastore、Apache Atlas）记录数据的位置、格式、Schema、血缘关系，实现数据资产的全局可见。

• Schema演进支持：
支持动态Schema（如JSON嵌套结构）和Schema版本控制，适应半结构化数据需求。

3. 计算层（灵活分析能力）

• 解耦存储与计算：

  ◦ 批处理引擎：Spark、Hive（处理TB/PB级离线数据）

  ◦ 交互式查询：Presto、Trino、AWS Athena（秒级SQL响应）

  ◦ 流式计算：Flink、Spark Streaming（实时数据入湖与分析）

• 统一计算框架：
Databricks、Snowflake等平台支持多种计算模式，避免数据迁移。

4. 数据治理与安全层（合规性保障）

• 数据治理工具：

  ◦ 数据目录：Alation、Collibra（业务标签与数据搜索）

  ◦ 血缘追踪：记录数据加工链路（如Apache Lineage）

  ◦ 质量监控：Great Expectations、Deequ（自动校验数据规则）

• 安全控制：

  ◦ 权限管理：基于角色的访问控制（RBAC）

  ◦ 加密：静态加密（AES-256）、传输加密（TLS）

  ◦ 审计：操作日志与合规报告（如AWS CloudTrail）

5. 数据接入与处理层（全链路集成）

• 多源数据接入：

  ◦ 批量导入：Sqoop、Airflow（数据库离线同步）

  ◦ 实时流式：Kafka、AWS Kinesis（日志/事件实时入湖）

  ◦ IoT设备：MQTT、边缘计算网关（传感器数据直连）

• ETL/ELT工具：
dbt、AWS Glue、Apache NiFi实现数据清洗与转换。

6. 应用层（价值输出）

• BI与可视化：
Tableau、Power BI直接查询湖内数据生成报表。

• AI/ML平台：
数据湖作为特征库，支持TensorFlow、PyTorch训练模型（如AWS SageMaker）。

• 数据服务化：
通过API网关（如AWS API Gateway）将数据以服务形式开放。

关键技术突破

• 开放表格式：
Delta Lake、Apache Iceberg、Hudi提供ACID事务支持，解决数据一致性痛点。

• 智能分层存储：
冷热数据自动分级（如AWS S3 Intelligent-Tiering），降低存储成本。

• Serverless查询：
无服务器化计算（如BigQuery、AWS Athena）按需付费，降低运维复杂度。

结构示例图

[数据源] → [接入层] → (存储层+元数据) → [计算引擎] → [应用层]  
           ↑↓ 治理与安全贯穿全流程


与传统数据仓库对比

维度	数据湖	传统数据仓库
数据类型	原始格式（结构/半结构/非结构）	高度结构化
Schema策略	读时模式（Schema-on-Read）	写时模式（Schema-on-Write）
扩展性	云原生无限扩展	有限垂直扩展
成本	存储成本低，计算弹性	软硬件成本高

总结：现代数据湖通过存储计算分离、开放格式、统一元数据三大支柱，构建了灵活、可扩展的数据基础架构，成为企业数字化转型的核心基座。其结构设计始终围绕存得住、找得到、算得快、管得好四大目标演进。
