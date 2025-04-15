from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging

class BreweryDataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        
    def create_bronze_layer(self, raw_data: List[Dict], output_path: str) -> DataFrame:
        """Create bronze layer with raw data"""
        df = self.spark.createDataFrame(raw_data)
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", lit(current_timestamp()))
        df = df.withColumn("data_source", lit("openbrewerydb_api"))
        
        # Write as JSON with date partitioning
        df.write.mode("overwrite").json(f"{output_path}/bronze/date={datetime.today().strftime('%Y-%m-%d')}")
        
        return df
        
    def create_silver_layer(self, bronze_df: DataFrame, output_path: str) -> DataFrame:
        """Transform to silver layer with proper schema and partitioning"""
        # Select and rename columns
        silver_df = bronze_df.select(
            col("id").alias("brewery_id"),
            col("name"),
            col("brewery_type").alias("type"),
            col("street"),
            col("city"),
            col("state"),
            col("postal_code").alias("zip_code"),
            col("country"),
            col("longitude").cast(FloatType()),
            col("latitude").cast(FloatType()),
            col("phone"),
            col("website_url").alias("website"),
            col("ingestion_timestamp")
        )
        
        # Clean data
        silver_df = silver_df.na.fill({"country": "United States"})
        
        # Write as parquet partitioned by state and city
        (silver_df.write.mode("overwrite")
            .partitionBy("state", "city")
            .parquet(f"{output_path}/silver"))
            
        return silver_df
        
    def create_gold_layer(self, silver_df: DataFrame, output_path: str) -> DataFrame:
        """Create aggregated gold layer"""
        aggregated_df = (silver_df.groupBy("state", "city", "type")
            .agg(count("*").alias("brewery_count"))
            .orderBy("state", "city", "type"))
            
        # Write as delta format
        (aggregated_df.write.mode("overwrite")
            .format("delta")
            .save(f"{output_path}/gold/breweries_by_type_location"))
            
        return aggregated_df