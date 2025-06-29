import os
from pathlib import Path


class Settings:
    # OSS配置
    OSS_ENDPOINT = os.getenv("OSS_ENDPOINT", "your-oss-endpoint")
    OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID", "your-access-key-id")
    OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET", "your-access-key-secret")
    OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME", "your-bucket-name")

    # ClickHouse配置
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "root1")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "ea907")

    # 数据模拟配置
    SIMULATE_DAYS = 30
    SIMULATE_CATEGORIES = ['食品', '家居', '数码', '服饰']
    SIMULATE_ITEMS_PER_CAT = 50
    SIMULATE_PRICE_RANGE = (10, 100)
    CATEGORY_WEIGHTS = {'食品': 0.4, '家居': 0.2, '数码': 0.2, '服饰': 0.2}

    # 路径配置
    DATA_DIR = Path(__file__).parent.parent / "data"
    DATA_DIR.mkdir(exist_ok=True)


settings = Settings()