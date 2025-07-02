# 大数据技术-课时1-简介

[课时1PPT](https://github.com/quanttide/qtclass-profile-of-scst-zstu/blob/main/courses/big_data/theories/content/course_PPT/%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%8A%80%E6%9C%AF-%E8%AF%BE%E6%97%B61-%E7%AE%80%E4%BB%8B.pptx)

## 知识背景
- **数据库**：MySQL，SQL语法。
- **操作系统**：大三课程 Linux
- **分布式系统**：听说过概念

---

## 早期基础设施
### Google的三篇论文（未开源对应软件）
- **GFS**
- **BigTable**
- **MapReduce**
  
### 雅虎发起的Hadoop开源实现
| 论文  | Hadoop实现 | 说明 |
|-------|------------|------|
| GFS   | **HDFS**（Hadoop File System） | 存储非结构化数据 |
| MapReduce | **MapReduce** | 计算引擎：<br>- **Map**：分配到多台机器分别计算<br>（例：1楼一百万本，2楼二百万本，3楼三百万本）<br>- **Reduce**：统计结果（六百万本） |
| BigTable | **HBase** | HDFS上层结构化查询引擎 |

#### 演进补充：
- MapReduce → **Spark**（内存计算更快，资源消耗较大）
- Hadoop + **Hive**：通过Hive SQL转换层实现SQL操作 → 最早的数据仓库

---

## 现代基础设施
### 存储演进
| 早期技术 | 现代方案 | 特点 |
|----------|----------|------|
| HDFS     | **对象存储/文件存储** | 海量数据存储 |
| HBase    | **数据仓库** | 基于MySQL/PostgreSQL实现，OLTP改OLAP |
| Hive     | **数据湖** | 对象存储上运行SQL |

### 计算调度演进
| 早期技术 | 现代方案 | 特点 |
|----------|----------|------|
| MapReduce/Spark | **云原生/Serverless** | 容器（Docker镜像）、k8s调度、函数计算 |

### 架构融合
- **数据仓库** + **数据湖** → **湖仓一体（Lakehouse）**
  - **基础设施**：Iceberg
  - **厂商**：Snowflake、Databricks

---

## 核心概念
### 1. OLAP vs OLTP的区别
- **OLTP**：在线事务处理（如MySQL）
- **OLAP**：在线分析处理（如数据仓库）

### 2. 对象存储
- **协议**：S3协议

### 3. 数据湖
- **组成**：对象存储 + Spark/Flink计算引擎

### 4. 湖仓一体
- **定位**：数据仓库与数据湖的结合体


