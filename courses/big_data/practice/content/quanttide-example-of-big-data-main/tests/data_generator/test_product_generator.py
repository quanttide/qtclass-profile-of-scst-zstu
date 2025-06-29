import unittest
import csv
import tempfile
from typing import Set
from src.data_generator.product_generator import *

class TestProductGenerator(unittest.TestCase):



    def test_load_leaf_categories(self):
        """测试分类加载功能"""
        categories = load_leaf_categories()
        self.assertEqual(len(categories), 5)
        self.assertIsInstance(categories[0], Category)
        self.assertEqual(categories[1].category_name, "粮食")

    def test_generate_unique_product_id(self):
        """测试唯一ID生成"""
        existing: Set[int] = {123456789012}
        new_id = generate_unique_product_id(existing)
        self.assertNotIn(new_id, existing)
        self.assertTrue(100000000000 <= new_id <= 999999999999)

    def test_product_name_generation(self):
        """测试商品名称生成逻辑"""
        name = generate_product_name("Electronics", 5)
        self.assertEqual(name, "Electronics_5")

    def test_price_generation(self):
        """测试价格波动范围"""
        base = 100.0
        price = generate_initial_price(base)
        self.assertTrue(70.0 <= price <= 130.0)
        self.assertEqual(len(str(price).split('.')[1]), 2)  # 验证小数点后两位

    def test_weight_generation(self):
        """测试重量生成范围"""
        weight = generate_weight()
        self.assertTrue(0.1 <= weight <= 1.0)
        self.assertEqual(len(str(weight).split('.')[1]), 6)  # 验证小数点后六位

    def test_product_pool_generation(self):
        """测试完整商品池生成"""
        categories = load_leaf_categories()
        products = generate_product_pool(categories, 100)

        self.assertEqual(len(products), 100)
        self.assertEqual(len({p.product_id for p in products}), 100)  # 验证ID唯一性

        # 验证分类分布
        category_counts = {c.category_id:0 for c in categories}
        for p in products:
            category_counts[p.category_id] += 1
        self.assertTrue(all(20 <= count <= 80 for count in category_counts.values()))

    def test_csv_writing(self):
        """测试CSV写入功能"""
        test_products = [
            Product(123, 1, "Test", 0.5, 99.99, 0),
            Product(456, 2, "Test2", 0.7, 199.0, 1)
        ]

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            temp_path = temp_file.name  # 获取临时文件路径
            temp_file.close()  # 先关闭文件，避免锁定

            write_products_to_csv(test_products)  # 确保函数能接受文件路径

            # 重新打开文件进行验证
            with open(temp_path, 'r') as f:
                content = f.read()
                self.assertIn("123,1,Test,0.5,99.99,0", content)
                self.assertIn("456,2,Test2,0.7,199.0,1", content)

        # 测试完成后手动删除临时文件
        os.unlink(temp_path)

if __name__ == '__main__':
    unittest.main()
