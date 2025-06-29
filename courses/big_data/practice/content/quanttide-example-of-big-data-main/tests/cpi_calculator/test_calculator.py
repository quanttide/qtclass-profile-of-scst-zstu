import unittest
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np
# 在文件开头添加（第5行后插入）
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))  # 定位到项目根目录


class TestDailyCPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 创建临时测试数据目录
        cls.test_dir = Path(__file__).parent / 'test_data'
        cls.test_dir.mkdir(exist_ok=True)

        # 创建测试数据
        cls._create_test_categories()
        cls._create_test_products()
        cls._create_test_prices()

    @classmethod
    def _create_test_categories(cls):
        """创建测试分类数据"""
        categories = pd.DataFrame({
            'category_id': [1001, 1002],
            'parent': [None, None],  # 两个叶子分类
            'weight': [0.6, 0.4]
        })
        categories.to_csv(cls.test_dir / 'categories.csv', index=False)

    @classmethod
    def _create_test_products(cls):
        """创建测试产品数据"""
        products = pd.DataFrame({
            'product_id': [1, 2],
            'category_id': [1001, 1002]
        })
        products.to_csv(cls.test_dir / 'products.csv', index=False)

    @classmethod
    def _create_test_prices(cls):
        """生成5天测试价格数据（每天+1%涨幅）"""
        price_dir = cls.test_dir / 'daily_price'
        price_dir.mkdir(exist_ok=True)

        base_prices = {1: 100.0, 2: 200.0}

        for day in range(5):
            current_date = date(2025, 5, 1) + timedelta(days=day)
            prices = pd.DataFrame({
                'product_id': [1, 2],
                'price': [base_prices[1] * (1.01 ** day), base_prices[2] * (1.01 ** day)]
            })
            prices.to_csv(price_dir / f'daily_prices_{current_date.strftime("%Y%m%d")}.csv', index=False)

    def test_daily_cpi_output(self):
        """验证每日CPI数组的完整性"""
        from cpi_calculator.calculator import PandasCPICalculator  # 根据实际导入路径调整

        # 初始化计算器
        calculator = PandasCPICalculator(self.test_dir)

        # 测试日期范围
        start_date = date(2025, 5, 1)
        end_date = date(2025, 5, 4)
        actual = calculator.compute_daily_cpi(start_date, end_date)

        # 验证类型和索引
        self.assertIsInstance(actual, pd.Series, "应该返回Series")
        self.assertTrue(
            actual.index.equals(pd.date_range(start_date, end_date)),
            "索引应为连续日期"
        )

        # 验证数值计算（允许0.0001的误差）
        expected = pd.Series(
            [1.0000, 1.0100, 1.0201, 1.0303],  # (1.01^0, 1.01^1, 1.01^2, 1.01^3)
            index=pd.date_range(start_date, end_date),
            name='CPI'
        ).round(4)

        pd.testing.assert_series_equal(actual, expected, check_names=False)

    def test_single_day_case(self):
        """测试单日边界情况"""
        from cpi_calculator.calculator import PandasCPICalculator

        calculator = PandasCPICalculator(self.test_dir)
        single_day = date(2025, 5, 1)
        result = calculator.compute_daily_cpi(single_day, single_day)

        self.assertEqual(len(result), 1, "单日应返回单个结果")
        self.assertEqual(result.iloc[0], 1.0, "基期CPI应为1.0")


if __name__ == '__main__':
    unittest.main()
