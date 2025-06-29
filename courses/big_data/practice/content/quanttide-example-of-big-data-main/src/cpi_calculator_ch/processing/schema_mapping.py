from typing import Dict, Any
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA


class SchemaMapper:
    def map_category_schema(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """映射分类数据模式"""
        mapped = {}
        for field, type_ in zip(CATEGORY_SCHEMA["fields"], CATEGORY_SCHEMA["types"]):
            if field in data:
                mapped[field] = self._convert_type(data[field], type_)
        return mapped

    def map_price_schema(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """映射价格数据模式"""
        mapped = {}
        for field, type_ in zip(PRICE_SCHEMA["fields"], PRICE_SCHEMA["types"]):
            if field in data:
                mapped[field] = self._convert_type(data[field], type_)
        return mapped

    def _convert_type(self, value, target_type: str):
        """类型转换"""
        if value is None:
            return None

        type_lower = target_type.lower()

        if "uint" in type_lower:
            return int(value)
        elif "int" in type_lower:
            return int(value)
        elif "float" in type_lower:
            return float(value)
        elif "date" in type_lower:
            return str(value)  # ClickHouse会自动转换
        elif "string" in type_lower:
            return str(value)
        else:
            return value