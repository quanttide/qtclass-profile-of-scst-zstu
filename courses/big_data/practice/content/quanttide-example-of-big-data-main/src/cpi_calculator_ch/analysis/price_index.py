import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Union
from storage.clickhouse_connector import ClickHouseConnector
from pathlib import Path
import os


class PriceIndexCalculator:
    def __init__(self, ch_connector: ClickHouseConnector = None):
        self.logger = logging.getLogger('price_index')
        self.ch = ch_connector or ClickHouseConnector()

    def calculate_cavallo_index(
            self,
            base_mode: str = 'auto',
            base_date: Optional[str] = None
    ) -> List[Dict[str, Union[str, float]]]:
        """
        计算Cavallo价格指数（基于几何平均法）

        参数:
            base_mode: 基期模式
                - 'auto': 使用数据首日作为基期
                - 'monthly': 每月首日作为新基期
                - 'fixed': 使用指定的base_date作为固定基期
            base_date: 当base_mode='fixed'时指定的固定基期日期(YYYY-MM-DD)

        返回:
            [{'date': '2025-01-01', 'index': 100.0, 'base_date': '2025-01-01'}, ...]
        """
        try:
            self.logger.info(f"Calculating Cavallo index with mode: {base_mode}")

            # 获取所有价格数据
            price_data = self._get_all_price_data()
            if not price_data:
                self.logger.warning("No price data available for Cavallo index")
                return []

            df = pd.DataFrame(price_data)
            df['date'] = pd.to_datetime(df['date'])

            results = []

            if base_mode == 'auto':
                # 模式1：整个周期使用首日作为基期
                base_date = df['date'].min()
                base_prices = self._get_base_prices(df, base_date)

                for date, group in df.groupby('date'):
                    index_value = self._calculate_geo_mean_index(group, base_prices)
                    results.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'index': index_value,
                        'base_date': base_date.strftime('%Y-%m-%d')
                    })

            elif base_mode == 'monthly':
                # 模式2：每月首日作为新基期
                monthly_bases = self._get_monthly_base_dates(df)

                for base_start in monthly_bases:
                    base_prices = self._get_base_prices(df, base_start)
                    month_end = (base_start + pd.offsets.MonthEnd(0)).date()

                    period_df = df[df['date'] >= base_start]
                    for date, group in period_df.groupby('date'):
                        if pd.to_datetime(date).date() > month_end:
                            continue

                        index_value = self._calculate_geo_mean_index(group, base_prices)
                        results.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'index': index_value,
                            'base_date': base_start.strftime('%Y-%m-%d')
                        })

            elif base_mode == 'fixed' and base_date:
                # 模式3：固定基期
                base_date = pd.to_datetime(base_date)
                base_prices = self._get_base_prices(df, base_date)

                for date, group in df.groupby('date'):
                    index_value = self._calculate_geo_mean_index(group, base_prices)
                    results.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'index': index_value,
                        'base_date': base_date.strftime('%Y-%m-%d')
                    })

            # 保存指数数据
            self._save_indices_to_csv(results, "cavallo_index.csv")

            return sorted(results, key=lambda x: x['date'])

        except Exception as e:
            self.logger.error(f"Failed to calculate Cavallo index: {str(e)}", exc_info=True)
            return []

    def calculate_tmall_index(
            self,
            base_mode: str = 'auto',
            base_date: Optional[str] = None
    ) -> List[Dict[str, Union[str, float]]]:
        """
        计算Tmall价格指数（基于加权平均法）

        参数:
            base_mode: 基期模式
                - 'auto': 使用数据首日作为基期
                - 'monthly': 每月首日作为新基期
                - 'fixed': 使用指定的base_date作为固定基期
            base_date: 当base_mode='fixed'时指定的固定基期日期(YYYY-MM-DD)

        返回:
            [{'date': '2025-01-01', 'index': 100.0, 'base_date': '2025-01-01'}, ...]
        """
        try:
            self.logger.info(f"Calculating Tmall index with mode: {base_mode}")

            # 获取分类权重
            weights = self._get_category_weights()
            if not weights:
                self.logger.error("No category weights found")
                return []

            # 获取每日分类聚合数据
            daily_data = self._get_daily_category_data()
            if not daily_data:
                self.logger.warning("No daily aggregated data found")
                return []

            df = pd.DataFrame(daily_data)
            df['date'] = pd.to_datetime(df['date'])

            results = []

            if base_mode == 'auto':
                # 模式1：整个周期使用首日作为基期
                base_date = df['date'].min()
                base_values = self._get_base_category_values(df, base_date)

                for date, group in df.groupby('date'):
                    index_value = self._calculate_weighted_index(group, base_values, weights)
                    results.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'index': index_value,
                        'base_date': base_date.strftime('%Y-%m-%d')
                    })

            elif base_mode == 'monthly':
                # 模式2：每月首日作为新基期
                monthly_bases = self._get_monthly_base_dates(df)

                for base_start in monthly_bases:
                    base_values = self._get_base_category_values(df, base_start)
                    month_end = (base_start + pd.offsets.MonthEnd(0)).date()

                    period_df = df[df['date'] >= base_start]
                    for date, group in period_df.groupby('date'):
                        if pd.to_datetime(date).date() > month_end:
                            continue

                        index_value = self._calculate_weighted_index(group, base_values, weights)
                        results.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'index': index_value,
                            'base_date': base_start.strftime('%Y-%m-%d')
                        })

            elif base_mode == 'fixed' and base_date:
                # 模式3：固定基期
                base_date = pd.to_datetime(base_date)
                base_values = self._get_base_category_values(df, base_date)

                for date, group in df.groupby('date'):
                    index_value = self._calculate_weighted_index(group, base_values, weights)
                    results.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'index': index_value,
                        'base_date': base_date.strftime('%Y-%m-%d')
                    })

            # 保存指数数据
            self._save_indices_to_csv(results, "tmall_index.csv")

            return sorted(results, key=lambda x: x['date'])

        except Exception as e:
            self.logger.error(f"Failed to calculate Tmall index: {str(e)}", exc_info=True)
            return []

    def _save_indices_to_csv(self, indices: List[Dict], filename: str) -> bool:
        """
        将指数数据保存到CSV文件
        参数:
            indices: 指数数据列表
            filename: 保存的文件名
        返回:
            bool: 是否保存成功
        """
        try:
            if not indices:
                self.logger.warning(f"No indices to save for {filename}")
                return False

            # 确保data目录存在
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)

            # 转换为DataFrame
            df = pd.DataFrame(indices)

            # 保存到CSV
            filepath = data_dir / filename
            df.to_csv(filepath, index=False)
            self.logger.info(f"Successfully saved indices to {filepath}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save indices to {filename}: {str(e)}")
            return False

    # [其余工具方法保持不变...]
    def _get_all_price_data(self) -> List[Dict]:
        """获取所有价格数据"""
        query = """
                SELECT toDate(date) as date,
                item_id,
                price
                FROM price
                ORDER BY date \
                """
        return self.ch.execute_query(query)

    def _get_daily_category_data(self) -> List[Dict]:
        """获取每日分类聚合数据（移除注释）"""
        query = """
                SELECT toDate(p.date) as date,
                i.category_id as category_id,  -- 使用SQL标准注释
                c.name as category_name,
                c.weight,
                avg(p.price) as avg_price,
                count(p.item_id) as item_count
                FROM price p
                    JOIN item i \
                ON p.item_id = i.item_id
                    JOIN category c ON i.category_id = c.category_id
                GROUP BY p.date, i.category_id, c.name, c.weight
                ORDER BY p.date \
                """
        return self.ch.execute_query(query)

    def _get_category_weights(self) -> Dict[int, float]:
        """获取分类权重字典 {category_id: weight}"""
        query = "SELECT category_id, weight FROM category"
        rows = self.ch.execute_query(query)
        return {row['category_id']: row['weight'] for row in rows}

    def _get_base_prices(self, df: pd.DataFrame, base_date: datetime) -> Dict[str, float]:
        """获取基期价格字典 {item_id: price}"""
        base_df = df[df['date'] == base_date]
        return dict(zip(base_df['item_id'], base_df['price']))

    def _get_base_category_values(self, df: pd.DataFrame, base_date: datetime) -> Dict[int, float]:
        """获取基期分类平均价格 {category_id: avg_price}"""
        base_df = df[df['date'] == base_date]
        return dict(zip(base_df['category_id'], base_df['avg_price']))

    def _get_monthly_base_dates(self, df: pd.DataFrame) -> List[datetime]:
        """获取每月首日日期列表"""
        return list(df.groupby([df['date'].dt.year, df['date'].dt.month])['date'].min())

    def _calculate_geo_mean_index(self,
                                  daily_group: pd.DataFrame,
                                  base_prices: Dict[str, float]) -> float:
        """计算几何平均指数"""
        ratios = []
        for _, row in daily_group.iterrows():
            if row['item_id'] in base_prices and base_prices[row['item_id']] > 0:
                ratios.append(row['price'] / base_prices[row['item_id']])

        if not ratios:
            return 0.0

        product = 1.0
        for r in ratios:
            product *= r
        return round((product ** (1.0 / len(ratios))) * 100, 4)

    def _calculate_weighted_index(self,
                                  daily_group: pd.DataFrame,
                                  base_values: Dict[int, float],
                                  weights: Dict[int, float]) -> float:
        """计算加权平均指数"""
        weighted_sum = 0.0
        total_weight = 0.0

        for _, row in daily_group.iterrows():
            cat_id = row['category_id']
            if cat_id in base_values and base_values[cat_id] > 0:
                price_ratio = row['avg_price'] / base_values[cat_id]
                weighted_sum += price_ratio * weights.get(cat_id, 0)
                total_weight += weights.get(cat_id, 0)

        return round((weighted_sum / total_weight) * 100, 4) if total_weight > 0 else 0.0

    # [其余验证方法保持不变...]
    def validate_data_ready(self) -> bool:
        """验证数据是否准备好计算指数"""
        checks = [
            self._check_price_data_exists(),
            self._check_category_weights(),
            self._check_base_date_coverage()
        ]
        return all(checks)

    def _check_price_data_exists(self) -> bool:
        """检查价格数据是否存在"""
        count = self.ch.execute_query("SELECT count() FROM price")[0]['count()']
        if count == 0:
            self.logger.error("No price data found in database")
            return False
        return True

    def _check_category_weights(self) -> bool:
        """检查分类权重是否有效"""
        weights = self._get_category_weights()
        if not weights:
            self.logger.error("No category weights found")
            return False

        total_weight = sum(weights.values())
        if not (0.99 <= total_weight <= 1.01):  # 允许1%的误差
            self.logger.error(f"Invalid total weight: {total_weight}")
            return False

        return True

    def _check_base_date_coverage(self) -> bool:
        """检查基期数据覆盖情况"""
        earliest_date = self.ch.execute_query(
            "SELECT toDate(min(date)) as min_date FROM price"
        )[0]['min_date']

        item_count = self.ch.execute_query(
            f"SELECT count(DISTINCT item_id) FROM price WHERE date = '{earliest_date}'"
        )[0]['count()']

        if item_count == 0:
            self.logger.error(f"No items found on base date {earliest_date}")
            return False

        return True