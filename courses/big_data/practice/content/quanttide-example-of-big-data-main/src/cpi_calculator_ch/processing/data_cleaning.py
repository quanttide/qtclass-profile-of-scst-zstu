import pandas as pd
from typing import Tuple, Optional


class DataCleaner:
    def __init__(self, unit_conversion=False):
        self.unit_conversion = unit_conversion  # 是否将价格单位转换（如元转分）

    def clean_category_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
        """清洗分类数据"""
        df_clean = df.dropna()

        # 限制权重在 0 ~ 1 之间
        df_clean["weight"] = df_clean["weight"].clip(0, 1)

        return df_clean, None

    def clean_item_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
        """清洗商品数据"""
        df_clean = df.dropna()

        # item_id 强制转为字符串格式
        df_clean["item_id"] = df_clean["item_id"].astype(str).str.strip().str.upper()

        return df_clean, None

    def clean_price_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
        """清洗价格数据"""
        df_clean = df.copy()

        # 价格必须大于0，不能为 NaN
        df_clean = df_clean[(df_clean["price"].notna()) & (df_clean["price"] > 0)]

        # 转换 item_id 为大写，去空格
        df_clean["item_id"] = df_clean["item_id"].astype(str).str.strip().str.upper()

        # 确保日期是日期类型
        df_clean["date"] = pd.to_datetime(df_clean["date"])

        df_clean = df_clean.reset_index(drop=True)

        if df_clean.empty:
            return None, "清洗后无有效价格数据"

        return df_clean, None
