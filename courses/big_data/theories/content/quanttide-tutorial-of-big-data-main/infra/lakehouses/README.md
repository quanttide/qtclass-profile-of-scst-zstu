# 湖仓一体

## 分层


Iceberg、OpenMedadata、Gravitino分别有什么不同？
Iceberg统一表格式，Gravitino提供一个统一Iceberg和其他结构化数据的方案；OpenMedadata用于业务上的数据治理。

+———————+
|   OpenMetadata      |  ← 元数据治理、血缘分析
+-———+-———+
           |
+-———v-———+
|     Gravitino       |  ← 联邦Iceberg、Hive、MySQL的元数据
+-———+-———+
           |
+-———v-———+
|  Iceberg (on S3)    |  ← 存储层ACID事务
+-———+-———+
           |
+-———v-———+
|  S3/Object Storage  |  ← 原始文件存储
+———————+

