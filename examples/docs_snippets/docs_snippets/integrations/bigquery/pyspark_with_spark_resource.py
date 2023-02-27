from dagster_gcp_pyspark import bigquery_pyspark_io_manager
from dagster_pyspark import pyspark_resource
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


@asset(required_resource_keys={"pyspark"})
def iris_data(context) -> DataFrame:
    spark = context.resources.pyspark.spark_session

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
        "pyspark": pyspark_resource.configured(
            {"spark_conf": {"spark.jars.packages": BIGQUERY_JARS}}
        ),
    },
)
