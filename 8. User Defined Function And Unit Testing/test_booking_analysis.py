# Unit Test booking analysis
import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from booking_analysis import top_3_revenue,read_booking_summary


# Test data
@pytest.fixture(scope="session")
def spark():
    #Global spark session passed as fixature
    return SparkSession.builder.getOrCreate()

def test_read_booking_summary(spark):
    #Get Actual Result
    summary_df = read_booking_summary(spark)
    record_loaded = summary_df.count()
    #Assert the actual expected result
    assert record_loaded == 58

def test_top_3_revenue(spark):
    #Get Actual Result
    summary_df = read_booking_summary(spark)
    result_df = top_3_revenue(summary_df)
    #Get expected result
    file_schema = "booked_by string, booking_date date, revenue double"
    expected_df =(
        spark.read.format("csv")
            .option("header","true")
            .schema(file_schema)
            .load("/Volumes/dev/spark_db/datasets/spark_programming/data/top-3-days-test-data.csv")
    )
    #assert
    assertDataFrameEqual(result_df, expected_df)


    