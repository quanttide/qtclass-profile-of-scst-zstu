import pandas as pd
from io import StringIO, BytesIO
from typing import Union
from config.cloud_settings import settings


class OSSConnector:
    """统一存储连接器（自动适配MinIO/OSS）"""

    def __init__(self):
        if settings.IS_LOCAL:
            self._init_minio()
        else:
            self._init_oss()

    def _init_minio(self):
        """初始化MinIO客户端"""
        from minio import Minio
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False
        )
        # 确保Bucket存在
        if not self.client.bucket_exists(settings.OSS_BUCKET):
            self.client.make_bucket(settings.OSS_BUCKET)

    def _init_oss(self):
        """初始化阿里云OSS客户端"""
        import oss2
        self.auth = oss2.Auth(settings.OSS_ACCESS_KEY, settings.OSS_SECRET_KEY)
        self.bucket = oss2.Bucket(
            self.auth,
            settings.OSS_ENDPOINT,
            settings.OSS_BUCKET
        )

    def upload_dataframe(self, df: pd.DataFrame, object_key: str, **kwargs) -> bool:
        """通用DataFrame上传方法"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, **kwargs)

            if settings.IS_LOCAL:
                self.client.put_object(
                    settings.OSS_BUCKET,
                    object_key,
                    BytesIO(csv_buffer.getvalue().encode()),
                    length=len(csv_buffer.getvalue())
                )
            else:
                self.bucket.put_object(object_key, csv_buffer.getvalue())

            return True
        except Exception as e:
            self._log_error(e)
            return False

    def download_dataframe(self, object_key: str, **kwargs) -> Union[pd.DataFrame, None]:
        """下载CSV为DataFrame"""
        try:
            if settings.IS_LOCAL:
                response = self.client.get_object(settings.OSS_BUCKET, object_key)
                data = response.read().decode('utf-8')
            else:
                data = self.bucket.get_object(object_key).read().decode('utf-8')

            return pd.read_csv(StringIO(data), **kwargs)
        except Exception as e:
            self._log_error(e)
            return None

    def _log_error(self, error: Exception):
        """统一错误处理"""
        import logging
        logging.error(f"存储操作失败: {str(error)}")
        if settings.ENV == "dev":
            import traceback
            traceback.print_exc()


# 使用示例
if __name__ == "__main__":
    connector = OSSConnector()
    test_df = pd.DataFrame({"test": [1, 2, 3]})

    if connector.upload_dataframe(test_df, "test_upload.csv"):
        print("上传成功")
        downloaded = connector.download_dataframe("test_upload.csv")
        print(downloaded)
