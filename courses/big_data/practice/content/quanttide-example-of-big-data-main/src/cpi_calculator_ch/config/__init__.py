import os
from pathlib import Path


class _Config:
    def __init__(self):
        self.env = os.getenv("CLICKHOUSE_ENV", "local")  # 默认本地环境

    @property
    def is_local(self):
        return self.env == "local"

    def __getattr__(self, name):
        # 动态加载配置
        
        from .cloud_settings import settings
        return getattr(settings, name)


config = _Config()
