# 数据表结构定义
CATEGORY_SCHEMA = {
    "fields": ["category_id", "name", "weight", "timestamp"],
    "types": ["UInt32", "String", "Float32", "DateTime"]
}

ITEM_SCHEMA = {
    "fields": ["item_id", "category_id"],
    "types": ["String", "UInt32"]
}

PRICE_SCHEMA = {
    "fields": ["date", "item_id", "price"],
    "types": ["Date", "String", "Float32"]
}
