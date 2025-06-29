import os
import logging
from clickhouse_driver import Client
from cloud_config import settings

class ClickHouseConnector:
    def __init__(self):
        self.logger = logging.getLogger('clickhouse_connector')
        self.client = self._create_client()
        self.logger.info(f"ClickHouse连接已建立 -> {settings.CH_HOST}:{settings.CH_PORT}")

    def _create_client(self):
        """创建适配阿里云的ClickHouse客户端"""
        # 基础连接参数
        conn_params = {
            'host': settings.CH_HOST,
            'port': settings.CH_PORT,
            'user': settings.CH_USER,
            'password': settings.CH_PASSWORD,
            'database': 'default'
        }

        # 阿里云专用配置
        if 'aliyuncs.com' in settings.CH_HOST.lower():
            conn_params.update({
                'secure': True,  # 启用SSL
                'verify': False,  # 阿里云通常使用自签名证书，设为False
                'connect_timeout': 15,
                'send_receive_timeout': 30,
                'settings': {
                    'insert_distributed_sync': 1,
                    'max_memory_usage': 10_000_000_000
                }
            })
            self.logger.debug("使用阿里云ClickHouse专用配置")

        return Client(**conn_params)

    def execute(self, query: str, params=None):
        """统一执行 SQL 查询接口"""
        try:
            self.logger.debug(f"Executing query: {query}")
            if params:
                result = self.client.execute(query, params)
            else:
                result = self.client.execute(query)
            return result
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def execute_query(self, query: str, params=None, return_dataframe: bool = False):
        """
        执行查询并返回格式化结果
        """
        try:
            self.logger.debug(f"Executing query: {query}")
            if params:
                result, columns = self.client.execute(query, params, with_column_types=True)
            else:
                result, columns = self.client.execute(query, with_column_types=True)

            if return_dataframe:
                import pandas as pd
                column_names = [col[0] for col in columns]
                return pd.DataFrame(result, columns=column_names)
            else:
                column_names = [col[0] for col in columns]
                return [dict(zip(column_names, row)) for row in result]

        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    def initialize_tables(self):
        """初始化数据库表格，创建 category、item 和 price 表"""
        try:
            # 创建 category 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS category
                                (
                                    category_id UInt32,
                                    name        String,
                                    weight      Float64,
                                    timestamp   DateTime
                                ) ENGINE = MergeTree()
                                      ORDER BY category_id;
                                ''')

            # 创建 item 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS item
                                (
                                    item_id     String,
                                    category_id UInt32
                                ) ENGINE = MergeTree()
                                      ORDER BY item_id;
                                ''')

            # 创建 price 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS price
                                (
                                    date    Date,
                                    item_id String,
                                    price   Float64
                                ) ENGINE = MergeTree()
                                      ORDER BY (date, item_id);
                                ''')

            self.logger.info("Tables initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize tables: {str(e)}")
            raise

    def insert_category(self, category_data):
        """批量插入类别数据"""
        try:
            query = '''
                    INSERT INTO category (category_id, name, weight, timestamp)
                    VALUES \
                    '''
            self.client.execute(query, category_data)
            self.logger.info(f"Inserted {len(category_data)} rows into category table.")
        except Exception as e:
            self.logger.error(f"Failed to insert category data: {str(e)}")
            raise

    def insert_item(self, item_data):
        """批量插入商品数据"""
        try:
            query = '''
                    INSERT INTO item (item_id, category_id)
                    VALUES \
                    '''
            self.client.execute(query, item_data)
            self.logger.info(f"Inserted {len(item_data)} rows into item table.")
        except Exception as e:
            self.logger.error(f"Failed to insert item data: {str(e)}")
            raise

    def insert_price(self, price_data):
        """批量插入价格数据"""
        try:
            query = '''
                    INSERT INTO price (date, item_id, price)
                    VALUES \
                    '''
            self.client.execute(query, price_data)
            self.logger.info(f"Inserted {len(price_data)} rows into price table.")
        except Exception as e:
            self.logger.error(f"Failed to insert price data: {str(e)}")
            raise

    def close(self):
        """关闭连接"""
        try:
            self.client.disconnect()
            self.logger.info("ClickHouse连接已关闭")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")
