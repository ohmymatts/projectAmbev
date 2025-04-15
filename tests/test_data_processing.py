# test_data_processing.py
import pytest
from pyspark.sql import SparkSession
from scripts.data_processing import BreweryDataProcessor
from pyspark.sql.types import StructType, StructField, StringType

@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("test")
            .getOrCreate())

def test_create_bronze_layer(spark):
    test_data = [{"id": "1", "name": "Test Brewery", "brewery_type": "micro"}]
    processor = BreweryDataProcessor(spark)
    df = processor.create_bronze_layer(test_data, "test_output")
    
    assert df.count() == 1
    assert "ingestion_timestamp" in df.columns

def test_create_silver_layer(spark):
    test_data = [{"id": "1", "name": "Test Brewery", "brewery_type": "micro", 
                 "city": "Portland", "state": "Oregon"}]
    processor = BreweryDataProcessor(spark)
    bronze_df = spark.createDataFrame(test_data)
    silver_df = processor.create_silver_layer(bronze_df, "test_output")
    
    assert "type" in silver_df.columns  # renamed from brewery_type
    assert silver_df.filter(col("city") == "Portland").count() == 1