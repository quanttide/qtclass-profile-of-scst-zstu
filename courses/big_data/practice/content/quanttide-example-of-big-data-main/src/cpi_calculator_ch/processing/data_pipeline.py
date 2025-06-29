class DataPipeline:
    def __init__(self):
        self.storage = OSSConnector()
        self.ch = ClickHouseConnector()

    def run_etl(self):
        """统一ETL流程"""
        # 1. 数据获取
        if config.is_local:
            df = self._generate_mock_data()  # 本地用模拟数据
        else:
            df = self.storage.download_dataframe("raw/data.csv")

        # 2. 数据处理（核心逻辑不变）
        df = self._clean_data(df)
        df = self._transform_data(df)

        # 3. 数据加载
        if config.is_local:
            self._load_to_local_ch(df)  # 本地快速导入
        else:
            self._load_to_cloud(df)  # 云上优化导入

    def _load_to_local_ch(self, df):
        """本地快速加载实现"""
        self.ch.execute("INSERT INTO table VALUES", df.to_dict('records'))

    def _load_to_cloud(self, df):
        """云端批量加载实现"""
        self.storage.upload_dataframe(df, "processed/data.parquet")
        self.ch.execute(f"""
        INSERT INTO table 
        SELECT * FROM s3(
            'https://{config.OSS_BUCKET}.{config.OSS_ENDPOINT}/processed/data.parquet',
            '{config.OSS_ACCESS_KEY}',
            '{config.OSS_SECRET_KEY}',
            'Parquet'
        )
        """)