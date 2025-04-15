import os
from pathlib import Path
from pyspark.conf import SparkConf
from typing import Dict, Any

class Settings:
    """Central configuration for the Brewery Data Pipeline"""
    
    # 1. Pipeline Configuration
    PIPELINE_NAME: str = "brewery_data_pipeline"
    RUN_FREQUENCY: str = "@daily"  # Airflow schedule interval
    
    # 2. API Configuration
    API_BASE_URL: str = "https://api.openbrewerydb.org/v1/breweries"
    API_MAX_RETRIES: int = 3
    API_TIMEOUT: int = 30  # seconds
    API_PAGE_SIZE: int = 200  # Max records per request
    
    # 3. Data Lake Paths (default to local for development)
    DATA_LAKE_ROOT: str = os.getenv("DATA_LAKE_ROOT", "/data/brewery_data")
    
    @property
    def BRONZE_LAYER_PATH(self) -> str:
        return f"{self.DATA_LAKE_ROOT}/bronze"
    
    @property
    def SILVER_LAYER_PATH(self) -> str:
        return f"{self.DATA_LAKE_ROOT}/silver"
    
    @property
    def GOLD_LAYER_PATH(self) -> str:
        return f"{self.DATA_LAKE_ROOT}/gold"
    
    # 4. Spark Configuration
    @property
    def SPARK_CONFIG(self) -> Dict[str, Any]:
        return {
            "spark.app.name": self.PIPELINE_NAME,
            "spark.sql.shuffle.partitions": "5",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }
    
    def get_spark_conf(self) -> SparkConf:
        """Get Spark configuration object"""
        conf = SparkConf()
        for k, v in self.SPARK_CONFIG.items():
            conf.set(k, v)
        return conf
    
    # 5. Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # 6. Monitoring Configuration
    STATSD_HOST: str = os.getenv("STATSD_HOST", "localhost")
    STATSD_PORT: int = int(os.getenv("STATSD_PORT", 8125))
    STATSD_PREFIX: str = "brewery.pipeline"
    
    # 7. Environment Detection
    @property
    def ENVIRONMENT(self) -> str:
        return os.getenv("ENVIRONMENT", "development")
    
    @property
    def IS_PRODUCTION(self) -> bool:
        return self.ENVIRONMENT.lower() == "production"
    
    # 8. Data Quality Thresholds
    DATA_QUALITY_THRESHOLDS: Dict[str, float] = {
        "null_id_threshold": 0.0,       # 0% null IDs allowed
        "null_name_threshold": 0.05,    # 5% null names allowed
        "state_abbreviation_length": 2  # Expected state code length
    }

# Singleton instance
settings = Settings()