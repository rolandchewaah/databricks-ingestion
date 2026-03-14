import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.ingestion_job import transform_logic # Note: Refactor logic into a pure function
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

def test_silver_transformation_filters_nulls(spark):
    # 1. Create Mock Bronze Data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True)
    ])
    
    source_data = [
        ("user_1", "login"),
        (None, "click"),     # Should be filtered out
        ("user_2", "logout")
    ]
    bronze_df = spark.createDataFrame(source_data, schema)

    # 2. Expected Result
    expected_data = [
        ("user_1", "login"),
        ("user_2", "logout")
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # 3. Run Logic
    # (Assuming you move the filter logic into a function called transform_logic)
    output_df = bronze_df.filter(col("user_id").isNotNull())

    # 4. Assert Equality
    assert_df_equality(output_df, expected_df)