# 商品池生成器

## 数据结构设计

### 商品字段说明

每个商品对象包含以下字段：

| 字段名       | 类型            | 描述                                       |
|-------------|---------------|------------------------------------------|
| product_id  | INT           | 商品唯一标识符                                  |
| category_id | INT           | 商品所属分类（叶子分类）                             |
| name         | VARCHAR(50)   | 具体商品名称                                   |
| weight      | float         | 商品在三级分类中的权重，用于筛选，同一个三级分类里的商品weight加起来等于1 |
| price       | DECIMAL(12,2) | 商品当前价格（单位：元）                             |
| change_count| INT           | 商品价格变动次数                                 |

---

## 功能说明

### 主要功能
- 根据指定数量生成带有符合天猫规则的商品ID、该商品对应的分类(category_id)、商品名称(根据category_id,找出category_name,然后根据分类名称生成商品名称，比如分类名称叫苹果，商品名称就叫苹果_1，苹果_2)、商品初始价格(随机生成，同一种商品不要差别30%以上)、商品权重(商品在三级分类中的权重，用于筛选后续在price表中的筛选)、商品价格变动次数的商品池。
- 将生成的商品池写入 CSV 文件供后续处理使用。

---

## 函数设计
1. load_leaf_categories(file_path: str) → List[Category]
2. generate_unique_product_id(existing_ids: Set[int]) → int
3. generate_product_name(category_name: str, index: int) → str
4. generate_initial_price(base_price: float, fluctuation: float = 0.3) → float
5. generate_weight() → float
6. generate_change_count(max_changes: int = 5) → int
7. build_product(
       existing_ids: Set[int],
       category: Category,
       name_index: Dict[int, int]
   ) → Product
8. generate_product_pool(
       categories: List[Category],
       count: int
   ) → List[Product]
9. write_products_to_csv(
       products: List[Product],
       file_path: str
   ) → None

