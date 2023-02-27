from dagster_gcp_pyspark import bigquery_pyspark_io_manager
from pyspark import SparkFiles
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from dagster import Definitions, asset

BIGQUERY_JARS = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0"


@asset
def iris_data() -> DataFrame:
    spark = SparkSession.builder.config(
        key="spark.jars.packages",
        value=BIGQUERY_JARS,
    ).getOrCreate()

    schema = StructType(
        [
            StructField("Sepal length (cm)", DoubleType()),
            StructField("Sepal width (cm)", DoubleType()),
            StructField("Petal length (cm)", DoubleType()),
            StructField("Petal width (cm)", DoubleType()),
            StructField("Species", StringType()),
        ]
    )

    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
    spark.sparkContext.addFile(url)

    return spark.read.schema(schema).csv("file://" + SparkFiles.get("iris.data"))


defs = Definitions(
    assets=[iris_data],
    resources={
        "io_manager": bigquery_pyspark_io_manager.configured(
            {
                "project": "my-gcp-project",
                "dataset": "IRIS",
            }
        ),
    },
)
