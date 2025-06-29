import os
import warnings
from typing import Dict, Any, Optional


class CloudConfig:
    """统一配置中心（生产环境建议使用KMS或Vault管理敏感信息）"""

    # ============ 运行环境配置 ============
    ENV = os.getenv("APP_ENV", "dev")  # dev/test/prod
    IS_LOCAL = bool(os.getenv("IS_LOCAL", ""))

    # ============ 数据生成配置 ============
    SIMULATE_DAYS = 30
    SIMULATE_CATEGORIES = ['食品', '家居', '数码', '服饰']
    SIMULATE_ITEMS_PER_CAT = 50
    SIMULATE_PRICE_RANGE = (10, 100)
    CATEGORY_WEIGHTS = {'食品': 0.4, '家居': 0.2, '数码': 0.2, '服饰': 0.2}

    # ============ OSS统一配置 ============
    OSS_ENDPOINT = os.getenv("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com")
    OSS_BUCKET = os.getenv("OSS_BUCKET", "prod-ecommerce-data")
    OSS_ACCESS_KEY = os.getenv("OSS_ACCESS_KEY_ID")  # 必须设置
    OSS_SECRET_KEY = os.getenv("OSS_ACCESS_KEY_SECRET")

    # ============ MinIO本地开发配置 ============
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # ============ ClickHouse配置 ============
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "cc-bp143310x5229s4k4.public.clickhouse.ads.aliyuncs.com")
    CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "3306"))
    CH_USER = os.getenv("CLICKHOUSE_USER", "root1")
    CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD","Ea907e10dc8b")  # 必须设置

    # 连接参数（根据环境动态调整）
    @classmethod
    def get_ch_connect_args(cls) -> Dict[str, Any]:
        return {
            'secure': not cls.IS_LOCAL,
            'verify': cls.ENV == "prod",  # 仅生产环境验证证书
            'ca_certs': cls._get_ca_cert_path(),
            'settings': {
                'use_numpy': True,
                'connect_timeout': 30 if cls.ENV == "prod" else 60,
                'max_execution_time': 180
            }
        }

    @classmethod
    def _get_ca_cert_path(cls) -> Optional[str]:
        return '/etc/ssl/certs/ca-certificates.crt' if os.path.exists('/etc/ssl/certs/ca-certificates.crt') else None

    @classmethod
    def validate(cls):
        """配置完整性检查"""
        errors = []

        # OSS配置检查
        if not all([cls.OSS_ACCESS_KEY, cls.OSS_SECRET_KEY]):
            errors.append("OSS AccessKey/SecretKey 未设置")

        # ClickHouse检查
        if not cls.CH_PASSWORD:
            errors.append("ClickHouse 密码不能为空")

        # 本地开发检查
        if cls.IS_LOCAL and not all([cls.MINIO_ACCESS_KEY, cls.MINIO_SECRET_KEY]):
            errors.append("MinIO 凭证未配置")

        if errors:
            raise ValueError(f"配置验证失败: {', '.join(errors)}")


# 初始化配置
settings = CloudConfig()
CloudConfig.validate()
