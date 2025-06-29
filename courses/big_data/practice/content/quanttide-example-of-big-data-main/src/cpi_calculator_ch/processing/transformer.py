import pandas as pd
from config.constants import CATEGORY_SCHEMA, ITEM_SCHEMA, PRICE_SCHEMA


class DataTransformer:
    def transform_category_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换分类数据格式"""
        df["name"] = df["name"].astype(str).str.strip()

        # 权重归一化处理（总和为 1）
        total_weight = df["weight"].sum()
        if total_weight != 0:
            df["weight"] = df["weight"] / total_weight

        return df[CATEGORY_SCHEMA["fields"]]

    def transform_item_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换商品数据格式"""
        df["item_id"] = df["item_id"].astype(str).str.strip().str.upper()
        return df[ITEM_SCHEMA["fields"]]

    def transform_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换价格数据格式（修复版）"""
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["item_id"] = df["item_id"].astype(str).str.upper()

        # 修复点：保持原始浮点数值
        df["price"] = df["price"].astype(float).round(2)

        # 添加数据验证
        if (df["price"] > 1000).any():
            raise ValueError("发现异常高价，请检查原始数据")

        return df[PRICE_SCHEMA["fields"]]