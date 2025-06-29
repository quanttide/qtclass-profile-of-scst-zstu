import csv
import random
from typing import List, Set, Dict, NamedTuple
from pathlib import Path

class Category(NamedTuple):
    category_id: int
    category_name: str


class Product(NamedTuple):
    product_id: int
    category_id: int
    name: str
    weight: float
    price: float
    change_count: int = 0


# 加载叶子分类
def load_leaf_categories() -> List[Category]:
    """加载 hierarchy=3 的叶子分类"""
    file_path = Path(__file__).resolve().parent.parent.parent / 'data' / 'categories.csv'
    categories = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 确保 hierarchy 列存在且值为 '3'
            if 'hierarchy' in row and row['hierarchy'] == '3':
                categories.append(Category(int(row['category_id']), row['category']))
    return categories


# 生成唯一商品 ID
def generate_unique_product_id(existing_ids: Set[int]) -> int:
    while True:
        new_id = random.randint(100000000000, 999999999999)
        if new_id not in existing_ids:
            return new_id


# 生成商品名称
def generate_product_name(category_name: str, index: int) -> str:
    return f"{category_name}_{index}"


# 生成初始价格
def generate_initial_price(base_price: float, fluctuation: float = 0.3) -> float:
    min_price = base_price * (1 - fluctuation)
    max_price = base_price * (1 + fluctuation)
    return round(random.uniform(min_price, max_price), 2)


# 生成权重
def generate_weight() -> float:
    return round(random.uniform(0.1, 1.0), 6)


# 构建单条商品记录
def build_product(
    existing_ids: Set[int],
    category: Category,
    name_index: Dict[int, int],
    base_prices: Dict[int, float]
) -> Product:
    product_id = generate_unique_product_id(existing_ids)
    existing_ids.add(product_id)

    name_index[category.category_id] = name_index.get(category.category_id, 0) + 1
    name = generate_product_name(category.category_name, name_index[category.category_id])

    if category.category_id not in base_prices:
        base_prices[category.category_id] = random.uniform(10, 1000)

    price = generate_initial_price(base_prices[category.category_id])
    weight = generate_weight()

    return Product(
        product_id=product_id,
        category_id=category.category_id,
        name=name,
        weight=weight,
        price=price,
        change_count=0
    )


# 生成商品池
def generate_product_pool(categories: List[Category], count: int) -> List[Product]:
    products = []
    existing_ids = set()
    name_index = {}
    base_prices = {}

    per_category = count // len(categories)
    remainder = count % len(categories)

    # 首先生成所有商品（随机 weight）
    for category in categories:
        num = per_category + (1 if remainder > 0 else 0)
        remainder -= 1
        for _ in range(num):
            product = build_product(existing_ids, category, name_index, base_prices)
            products.append(product)

    # 按类别归一化 weight
    category_products: Dict[int, List[Product]] = {}
    for p in products:
        category_products.setdefault(p.category_id, []).append(p)

    # 处理每个类别
    for cat_id, prod_list in category_products.items():
        total_weight = sum(p.weight for p in prod_list)
        for i, p in enumerate(prod_list):
            new_weight = round(p.weight / total_weight, 6)
            prod_list[i] = p._replace(weight=new_weight)

    # 重新组合（因为 prod_list 是引用）
    normalized_products = []
    for cat_id in sorted(category_products.keys()):
        normalized_products.extend(category_products[cat_id])

    return normalized_products

# 写入 CSV
def write_products_to_csv(products: List[Product]) -> None:
    file_path = Path(__file__).resolve().parent.parent.parent / 'data' / 'products.csv'
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "category_id", "name", "weight", "price", "change_count"])
        for p in products:
            writer.writerow([p.product_id, p.category_id, p.name, p.weight, p.price, p.change_count])


# 示例用法
if __name__ == '__main__':
    category_file = 'categories.csv'  # 包含 category_id, category_name 的 CSV 文件
    output_file = 'product_pool.csv'
    total_products = 1000

    categories = load_leaf_categories()
    product_pool = generate_product_pool(categories, total_products)
    write_products_to_csv(product_pool)
