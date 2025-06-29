import random
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple, List
from collections import defaultdict

# 假设之前定义好的 Product 类型
class Product(NamedTuple):
    product_id: int
    category_id: int
    name: str
    weight: float
    price: float

class PriceGenerator:
    def __init__(self, products: List[Product]):
        # 初始化数据
        self.product_pool = products
        self.current_products = []
        self.promotion_dates = []
        # 整个生命周期只生成一次的价格变动计划
        self.price_change_plan = {}

    def weighted_random_choice(self, k_per_category: int = 120) -> List[Product]:
        category_map = defaultdict(list)
        for p in self.product_pool:
            category_map[p.category_id].append(p)

        selected = []
        for cat, items in category_map.items():
            weights = [p.weight for p in items]
            num_to_pick = min(len(items), k_per_category)
            picked = random.choices(items, weights=weights, k=num_to_pick)
            selected.extend(picked)

        return selected

    def produce_init(self) -> None:
        # 首次选品
        self.current_products = self.weighted_random_choice()

    def adjust_products(self) -> None:
        category_map = defaultdict(list)
        for p in self.current_products:
            category_map[p.category_id].append(p)

        new_products = []
        for cat_id, current_list in category_map.items():
            total = len(current_list)
            change_ratio = random.uniform(0.01, 0.02)
            change_num = max(1, int(total * change_ratio))

            # 保留当前未被替换的商品
            remove_indices = set(random.sample(range(total), change_num))
            kept = [p for i, p in enumerate(current_list) if i not in remove_indices]

            # 可选的新商品池：同类但不在当前池中的商品
            pool_candidates = [
                p for p in self.product_pool
                if p.category_id == cat_id and p not in current_list
            ]

            if pool_candidates:
                weights = [p.weight for p in pool_candidates]
                added = random.choices(pool_candidates, weights=weights, k=change_num)
            else:
                added = []

            new_products.extend(kept + added)

        self.current_products = new_products

    def init_price_plan(self, start_date: datetime, total_days: int = 365) -> None:
        """生成全年（或指定天数）每个产品的随机涨价日列表，只调用一次"""
        for p in self.current_products:
            # 高斯分布决定价格变动次数
            change_count = max(1, int(random.gauss(6, 2)))
            change_count = min(change_count, total_days)
            # 从第 1 天到 total_days-1 天中采样
            change_dates = sorted(random.sample(range(1, total_days), change_count))
            self.price_change_plan[p.product_id] = change_dates

    def adjust_prices(self, current_date: datetime, start_date: datetime) -> None:
        updated = []
        total_days = (current_date.date() - start_date.date()).days

        # 跳过第 0 天
        if total_days <= 0:
            return

        for p in self.current_products:
            plan = self.price_change_plan.get(p.product_id, [])
            if plan and plan[0] == total_days:
                # 到了预定变价日，弹出并生成新价格
                plan.pop(0)
                base_price = p.price
                new_price = round(base_price * random.uniform(0.9, 1.1), 2)
                updated.append(p._replace(price=new_price))
            else:
                updated.append(p)

        self.current_products = updated

def price_generator(products: List[Product], days: int = 365):
    gen = PriceGenerator(products)
    gen.produce_init()

    today = datetime.now()
    # 生成一次全年的价格变动计划
    gen.init_price_plan(today, total_days=days)

    out_dir = Path(__file__).parent.parent.parent / 'data' / 'daily_price'
    out_dir.mkdir(parents=True, exist_ok=True)

    for day in range(days):
        current = today + timedelta(days=day)
        if day > 0:
            gen.adjust_products()
            gen.adjust_prices(current, today)

        fn = out_dir / f"daily_prices_{current.strftime('%Y%m%d')}.csv"
        with fn.open('w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["product_id", "category_id", "name", "price", "change_date"])
            for p in gen.current_products:
                w.writerow([
                    p.product_id,
                    p.category_id,
                    p.name,
                    p.price,
                    current.strftime("%Y-%m-%d")
                ])

if __name__ == '__main__':
    # 示例：从 CSV 加载 product_pool 并生成 1 年数据
    products: List[Product] = []
    prod_csv = Path(__file__).parent.parent.parent / 'data' / 'products.csv'
    with prod_csv.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(Product(
                product_id=int(row['product_id']),
                category_id=int(row['category_id']),
                name=row['name'],
                weight=float(row['weight']),
                price=float(row['price']),
            ))

    price_generator(products, days=365)
    print("Finished! Files are under:",
          Path(__file__).parent.parent.parent / 'data' / 'daily_price')
