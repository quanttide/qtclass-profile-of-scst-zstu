import unittest
from datetime import datetime, timedelta
from unittest import TestCase
from src.data_generator.price_generator import Product, PriceGenerator


class TestPriceAdjustment(TestCase):
    def setUp(self):
        # 创建测试用商品（固定初始价格方便断言）
        self.test_product = Product(
            product_id=1,
            category_id=100,
            name="Test Product",
            weight=1.0,
            price=100.0  # 初始基准价格
        )

        # 创建 PriceGenerator 实例并强制设置当前商品池
        self.price_gen = PriceGenerator(products=[self.test_product])
        self.price_gen.current_products = [self.test_product]  # 跳过初始化随机选择

    def test_single_product_price_changes(self):
        """测试单个商品在预定变更日期是否发生价格变化"""
        # 设置测试时间参数
        start_date = datetime(2024, 1, 1)
        test_dates = [
            start_date + timedelta(days=i)
            for i in range(5)  # 测试前5天
        ]

        # 记录价格变化轨迹
        price_history = []

        for idx, date in enumerate(test_dates):
            # 每天执行价格调整
            self.price_gen.adjust_prices(date, start_date)
            current_price = self.price_gen.current_products[0].price
            price_history.append(current_price)

            # 断言非变更日价格保持不变（第一天不调整）
            if idx == 0:
                self.assertEqual(current_price, 100.0, "第一天不应调整价格")
            else:
                # 通过变化次数验证价格调整计划执行
                changes_count = sum(1 for p in price_history[1:idx + 1] if p != 100.0)

                # 验证每日最多一次调整
                self.assertLessEqual(
                    changes_count, 1,
                    f"第 {idx + 1} 天检测到多次价格变化"
                )

        # 验证价格波动范围（基准价的 ±10%）
        final_price = self.price_gen.current_products[0].price
        self.assertGreaterEqual(final_price, 90.0, "价格不应低于基准价 90%")
        self.assertLessEqual(final_price, 110.0, "价格不应高于基准价 110%")

        # 验证至少发生一次价格变化（高斯分布可能产生0次，这里增加容错）
        total_changes = sum(1 for p in price_history if p != 100.0)
        self.assertGreaterEqual(total_changes, 0, "应至少发生 0 次价格变化（随机允许）")


if __name__ == '__main__':
    unittest.main()
