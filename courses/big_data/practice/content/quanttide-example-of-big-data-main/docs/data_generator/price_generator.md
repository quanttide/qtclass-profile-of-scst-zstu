# 价格生成器
输入：product pool
输出：每天category下销量靠前的商品及商品的信息

## 生成程序的需求
从商品池每天每种产品抽取120个，抽取的结果每天变化1%-2%，被抽取的概率和权重相关。
商品的价格平均每1.5到2.5个月变化一次。大促的时间会额外增加变化。

## 数据模型
| 标识        | 格式           | 名称         | 描述                                       | 约束关系                          |
|-------------|----------------|--------------|--------------------------------------------|-------------------------------|
| date        | DATE           | 价格日期     | 价格记录日期（精确到天）                   | 主键（联合主键），非空                   |
| product_id  | INT            | 商品ID       | 商品                                       | 主键（联合主键），外键，非空                |
| name        | VARCHAR(50)    | 商品名称     | 具体商品名称                               |                               |
| price       | DECIMAL(12,2)  | 价格         | 商品当日价格（单位：元）                   | \>=0                          |

## 类划分、函数划分
class PriceGenerator:
    def __init__(self, product_pool):
        self.product_pool = product_pool
    def weighted_random_choice(products, k)
    def produce_init(self)
    def adjuct_products(self)
    def adjuct_prices(self)

def price_generator(product_pool):

## 价格更新逻辑

输入：起止日期
步骤：
1. 生成价格变化次数，比如一年平均6次，在这个范围内大概是4-8次，不同概率。
2. 根据基准价格，生成这么多个数量的价格和时间。
