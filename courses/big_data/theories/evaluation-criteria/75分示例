### 1.需求分析

#### 1.1目标
*   实现电商数据的采集、存储、清洗与建模。
*   利用 ClickHouse进行高效的加权价格聚合查询。
*   通过Python实现链式与固定基期价格指数的计算。
*   构建一套自动化、可复用、可扩展的指数计算系统。
*   输出直观的可视化结果图表

#### 1.2功能需求
*   能够将原始价格数据和分类数据上传并存储在阿里云OSS上;
*   使用 ClickHouse读取 OSS中的数据,并创建外部表进行管理;
    并在ClickHouse中对数据进行清洗,剔除价格缺失、价格为 0、销量为 0等异常记录;按日期与分类聚合计算加权平均价格;
*   使用 Python连接 ClickHouse实现指数计算逻辑,输出固定基期指数和链式指数;
*   绘制可视化图表,展示各分类的价格指数变化趋势,并将最终结果导出为CSV或写回 ClickHouse表中保存。

#### 1.3非功能需求
*   系统应具备良好的性能，查询与聚合操作应在秒级时间内完成;
*   指数计算过程应具备可重复性和可复用性，支持日常自动化调度;
*   模块之间应具备良好的解耦性，便于后期维护和功能拓展;
*   可视化输出应具备清晰、直观、易理解的展示效果;

### 2.运行环境
**云端环境**:阿里云(主运行平台)
*   阿里云oss
*   Clickhouse

**本地开发环境**:pycharm

### 3.使用工具
*   阿里云数据库 clickhouse
*   对象存储OSS
*   Pycharm

### 4.数据设计
**数据来源**:阿里云天池

#### 原始数据表结构
**price.csv**:包含商品每日价格、销量、商品ID、时间戳

| 字段名       | 类型     | 描述                  |
|--------------|----------|-----------------------|
| item_id      | String   | 商品唯一标识          |
| category_id  | String   | 商品所属分类ID        |
| date         | Date     | 日期(格式:YYYY-MM-DD) |
| price        | Float64  | 单日价格              |
| sales        | Int32    | 单日销量(用于加权)    |

**category.csv**:商品分类信息

| 字段名       | 类型     | 描述                         |
|--------------|----------|------------------------------|
| category_id  | String   | 商品分类ID                   |
| category_name| String   | 商品分类名称,如"家电"、"食品" |

### 5.脚本设计

#### 5.1 clickhouse代码脚本

##### 5.1.1创建表
首先创建OSS外部表存储数据

*   **价格数据 OSS外部表(price、category)**
```sql
CREATE TABLE oss_price(
    item_id String,
    category_id String,
    date Date,
    price Float64,
    sales Int32
) ENGINE=OSS(
    'oss-cn-xxx.aliyuncs.com',
    'your-bucket-name',
    'data/price.csv',
    'your-access-key-id',
    'your-access-key-secret'
);

CREATE TABLE oss_category(
    category_id String,
    category_name String
) ENGINE=OSS(
    'oss-cn-xxx.aliyuncs.com',
    'your-bucket-name',
    'data/category.csv',
    'your-access-key-id',
    'your-access-key-secret'
);
```
### 创建中间结果表
用以保存每类商品每日加权平均价格以及最终指数结果(链式与固定基期)
```sql
CREATE TABLE weighted_price_result(
    date Date,
    category_id String,
    weighted_avg_price Float64
) ENGINE= MergeTree()
ORDER BY(date, category_id);
```
### 5.1.2插入数据并清洗
```sql
INSERT INTO weighted_price_result
SELECT
    date,
    category_id,
    sum(price * sales) / sum(sales) AS weighted_avg_price
FROM oss_price
WHERE price > 0 AND sales > 0
GROUP BY date, category_id
ORDER BY date;
```
### 5.1.3测试结果
```sql
SELECT * FROM weighted_price_result
ORDER BY category_id, date
LIMIT 20;
```
### 5.2 Python脚本设计
用于查询clickhouse获取数据，执行链式、固定基数指数计算

#### 需要加载的库
```python
import pandas as pd
import matplotlib.pyplot as plt
from clickhouse_connect import get_client
```

### 连接ClickHouse
```python
# 连接 ClickHouse
client = get_client(
    host='localhost',
    port=8123,
    username='default',
    password=''
)
```
### 获取每日加权平均价格
```python
# 获取每日加权平均价格
query = """
SELECT
    date,
    category_id,
    weighted_avg_price
FROM weighted_price_result ORDER BY category_id, date
"""
df = client.query_df(query)
```

### 预处理
```python
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by=['category_id','date'])
```
### 固定基期指数计算
```python
def compute_fixed_index(df, category_col='category_id', price_col='weighted_avg_price'):
    df = df.copy()
    df['base_price'] = df.groupby(category_col)[price_col].transform('first')
    df['fixed_index'] = df[price_col] / df['base_price'] * 100
    return df[['date', category_col, 'fixed_index']]
    ```
### 链式指数计算
```python
def compute_chained_index(df, category_col='category_id', price_col='weighted_avg_price'):
    df = df.copy()
    df = df.sort_values(by=[category_col,'date'])
    df['index_ratio'] = df.groupby(category_col)[price_col].pct_change().fillna(0) + 1
    df['chained_index'] = df.groupby(category_col)['index_极认'].cumprod() * 100
    return df[['date', category_col, 'chained_index']]
    ```
    
### 执行计算
```python
# 执行计算
fixed_index_df = compute_fixed_index(df)
chained_index_df = compute_chained_index(df)

# 合并指数
final_df = pd.merge(fixed_index_df, chained_index_df, on=['date','category_id'])
final_df.to_csv('price_index_result.csv', index=False)
print("指数计算完成,结果保存在 price_index_result.csv")
```
### 5.2.1可视化

#### 各分类的指数曲线
```python
def plot_single_category(df, category_name):
    df_cat = df[df['category_id'] == category_name].sort_values('date')
    plt.figure(figsize=(10, 5))
    plt.plot(df_cat['date'], df_cat['fixed_index'], marker='o', label='固定基期指数')
    plt.plot(df_cat['date'], df_cat['chained_index'], marker='x', label='链式指数')
    plt.title(f'价格指数趋势图:{category_name}')
    plt.xlabel('日期')
    plt.ylabel('极格指数')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{category_name}_price_index.png')  # 可选:保存为图片
    plt.show()

plot_single_category(final_df, 'food')
```
### 所有分类的固定基期指数
```python
def plot_all_categories(df):
    plt.figure(figsize=(12, 6))
    for category in df['category_id'].unique():
        df_cat = df[df['category_id'] == category].sort_values('date')
        plt.plot(df_cat['date'], df_cat['fixed_index'], label=category)
    plt.title('各类商品价格指数(固定基期)')
    plt.xlabel('日期')
    plt.ylabel('价格指数')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('all_categories_index.png')  # 保存为图片
    plt.show()

plot_all_categories(final_df)
```
### 绘制总体价格指数
```python
df_total = pd.read_csv('overall_index.csv')
df_total['date'] = pd.to_datetime(df_total['event_date'])
plt.figure(figsize=(10, 5))
plt.plot(df_total['date'], df_total['overall_index'], color='green', marker='o')
plt.title('总体价格指数趋势图')
plt.xlabel('日期')
plt.ylabel('价格指数')
plt.grid(True)
plt.tight_layout()
plt.savefig('overall_price_index.png')
plt.show()
```

### 6.项目测试

#### 6.1单元测试
验证SQL查询是否能正确执行基本逻辑:字段解析、加权平均价格计算、异常数据过滤。通过构造小规模数据进行人工预估结果，与sql输出结果进行对比。

测试代码如下(sql):
```sql
SELECT
    date,
    category_id,
    sum(price * sales) / sum(sales) AS weighted_avg_price
FROM test_price
WHERE price > 0 AND sales > 0
GROUP BY date, category_id
ORDER BY date;
```

### 6.2集成测试
用于确保整个流程的正确运行，数据范围广泛有效。涵盖两个测试目的:

#### 6.2.1商品覆盖率≥80%
以验证计算中实际使用的商品数量是否覆盖了总商品的80%以上,确保价格指数的代表性,使用 sql:

```sql
-- 总商品数
SELECT count(DISTINCT item_id) AS total_items
FROM oss_price;

-- 被有效使用的商品数(价格和销量有效)
SELECT count(DISTINCT item_id) AS valid_items
FROM oss_price
WHERE price > 0 AND sales > 0;
```

### 6.2.1商品覆盖率≥80%
以验证计算中实际使用的商品数量是否覆盖了总商品的80%以上,确保价格指数的代表性,使用 sql:

```sql
-- 总商品数
SELECT count(DISTINCT item_id) AS total_items
FROM oss_price;

-- 被有效使用的商品数(价格和销量有效)
SELECT count(DISTINCT item_id) AS valid_items
FROM oss_price
WHERE price > 0 AND sales > 0;
```
覆盖率=valid_items/total_items × 100%

### 6.2.2异常过滤准确率≥95%
验证异常数据(价格为空或≤0,销量≤0)是否被成功过滤

```sql
-- 所有异常值的总数
SELECT count(*) AS total_abnormal
FROM oss_price
WHERE price IS NULL OR price <= 0 OR sales <= 0;

-- 被用于计算的剩余异常值(不应该有)
SELECT count(*) AS residual_abnormal
FROM weighted_price_result
WHERE weighted_avg_price IS NULL OR weighted_avg_price <= 0;
```
异常过滤准确率=(total_abnormal - residual_abnormal)/total_abnormal × 100%
