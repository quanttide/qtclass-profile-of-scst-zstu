# import clickhouse_driver
# import numpy as np
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from .schemas import Category, Price
# from .config import settings
#
# class CPICalculator:
#     def __init__(self, db_config):
#         self.db_config = db_config
#         self.clickhouse_client = self._connect_clickhouse()
#         self.sqlalchemy_engine = self._connect_sqlalchemy()
#         self.Session = sessionmaker(bind=self.sqlalchemy_engine)
#
#     def _connect_clickhouse(self):
#         """连接到 ClickHouse 数据库"""
#         return clickhouse_driver.Client(
#             host=self.db_config['CLICKHOUSE_HOST'],
#             port=self.db_config['CLICKHOUSE_PORT'],
#             user=self.db_config['CLICKHOUSE_USER'],
#             password=self.db_config['CLICKHOUSE_PASSWORD']
#         )
#
#     def _connect_sqlalchemy(self):
#         """连接到 SQLAlchemy 引擎"""
#         return create_engine(self.db_config['SQLALCHEMY_DATABASE_URI'])
#
#     def compute_cpi(self, start_date, end_date):
#         """计算指定日期范围内的消费者价格指数 (CPI)"""
#         # 使用 ClickHouse SQL 查询计算 CPI
#         sql_query = f"""
#         WITH
#         -- 获取所有叶子类别的ID和权重（没有子类别的类别）
#         leaf_categories AS (
#             SELECT id, weight
#             FROM category
#             WHERE NOT EXISTS (
#                 SELECT 1
#                 FROM category c2
#                 WHERE c2.parent = category.id
#             )
#         ),
#         -- 获取基期和报告期的价格（假设基期为上月，报告期为本月）
#         price_data AS (
#             SELECT
#                 product_id,
#                 MAX(CASE WHEN date = '{start_date}' THEN price END) AS base_price,
#                 MAX(CASE WHEN date = '{end_date}' THEN price END) AS report_price
#             FROM price
#             WHERE date IN ('{start_date}', '{end_date}')  -- 替换为实际日期
#             GROUP BY product_id
#         ),
#         -- 计算每个叶子类别的价格指数
#         category_cpi AS (
#             SELECT
#                 p.category_id,
#                 EXP(AVG(LN(pd.report_price / pd.base_price))) AS price_index  -- 几何平均数
#             FROM product p
#             JOIN price_data pd ON p.id = pd.product_id
#             JOIN leaf_categories lc ON p.category_id = lc.id
#             WHERE pd.base_price > 0  -- 确保分母不为零
#               AND pd.report_price IS NOT NULL
#             GROUP BY p.category_id
#         )
#         -- 计算加权CPI
#         SELECT
#             SUM(cc.price_index * lc.weight) AS CPI
#         FROM category_cpi cc
#         JOIN leaf_categories lc ON cc.category_id = lc.id;
#         """
#
#         result = self._execute_clickhouse_query(sql_query)
#         return result[0][0] if result else None
#
#     def _execute_clickhouse_query(self, query):
#         """执行 ClickHouse 查询"""
#         return self.clickhouse_client.execute(query)
#

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple
from datetime import date
import matplotlib.pyplot as plt

class PandasCPICalculator:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._load_data()

    def _load_data(self) -> None:
        """从CSV文件加载基础数据（仅加载必要字段）"""
        # 分类数据：只需要id, parent和weight
        self.categories = pd.read_csv(
            self.data_dir / 'categories.csv',
            usecols=['category_id', 'parent', 'weight']
        )

        # 产品数据：只需要id和category_id
        # （价格从daily_price获取，不需要products中的price）
        self.products = pd.read_csv(
            self.data_dir / 'products.csv',
            usecols=['product_id', 'category_id']
        )

        # 价格数据目录
        self.prices_dir = self.data_dir / 'daily_price'

    def _load_prices_for_dates(self, dates: Tuple[date, date]) -> pd.DataFrame:
        """加载指定日期的价格数据（自动添加日期列）"""
        price_dfs = []

        for target_date in dates:
            file_name = f"daily_prices_{target_date.strftime('%Y%m%d')}.csv"
            file_path = self.prices_dir / file_name

            if not file_path.exists():
                raise FileNotFoundError(f"Price file missing: {file_path}")

            df = pd.read_csv(file_path, usecols=['product_id', 'price'])
            df['date'] = target_date
            price_dfs.append(df)

        return pd.concat(price_dfs)

    def compute_daily_cpi(self, start_date: date, end_date: date) -> pd.Series:
        """计算每日CPI数组（相对基期的累计变化）"""
        # 获取叶子类别（没有子类别的分类）
        leaf_categories = self.categories[
            ~self.categories['category_id'].isin(self.categories['parent'].dropna())
        ][['category_id', 'weight']]

        # 加载完整时间范围的价格数据
        all_dates = pd.date_range(start_date, end_date, freq='D').date
        price_data = self._load_prices_for_dates(tuple(all_dates))

        # 创建完整价格透视表（填充缺失日期）
        price_pivot = price_data.pivot_table(
            index='product_id',
            columns='date',
            values='price',
            aggfunc='first'
        ).ffill(axis=1)  # 向前填充缺失价格

        # 获取基期价格（首日价格）
        base_prices = price_pivot[start_date].rename('base_price')

        # 合并产品信息
        merged_data = self.products.merge(
            base_prices,
            left_on='product_id',
            right_index=True
        ).merge(
            leaf_categories,
            on='category_id'
        )

        # 初始化结果存储
        cpi_series = pd.Series(index=all_dates, dtype='float64')

        # 逐日计算CPI
        for current_date in all_dates:
            # 合并当前日期价格
            current_prices = price_pivot[current_date].rename('current_price')
            daily_data = merged_data.merge(
                current_prices,
                left_on='product_id',
                right_index=True
            )

            # 过滤有效数据
            valid_data = daily_data[
                (daily_data['base_price'] > 0) &
                (daily_data['current_price'].notnull())
                ].copy()

            # 计算价格比率
            valid_data['price_ratio'] = valid_data['current_price'] / valid_data['base_price']
            valid_data['log_ratio'] = np.log(valid_data['price_ratio'])

            # 按类别计算几何平均
            category_index = valid_data.groupby('category_id')['log_ratio'].mean().apply(np.exp)

            # 合并权重计算当日CPI
            final_data = category_index.reset_index(name='price_index').merge(
                leaf_categories,
                on='category_id'
            )
            cpi_series[current_date] = (final_data['price_index'] * final_data['weight']).sum()

        return cpi_series.astype('float64').round(4)


def plot_cpi_trend(cpi_series: pd.Series):
    """绘制CPI趋势图"""
    plt.figure(figsize=(15, 6))

    # 绘制折线图
    cpi_series.plot(
        kind='line',
        title='Daily Consumer Price Index Trend',
        xlabel='Date',
        ylabel='CPI',
        grid=True,
        color='steelblue',
        marker='o',
        markersize=4
    )

    # 自动旋转日期标签
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.show()

# 使用示例
if __name__ == '__main__':
    # 获取数据目录路径
    data_dir = Path(__file__).resolve().parent.parent.parent / 'data'

    # 创建计算器实例
    calculator = PandasCPICalculator(data_dir)

    # 获取每日CPI数组
    start_date = date(2025, 5, 16)
    end_date = date(2026, 5, 15)
    daily_cpi = calculator.compute_daily_cpi(start_date, end_date)

    # 输出结果示例
    pd.set_option('display.max_rows', None)  # 不限制行数
    print(daily_cpi)
    plot_cpi_trend(daily_cpi)