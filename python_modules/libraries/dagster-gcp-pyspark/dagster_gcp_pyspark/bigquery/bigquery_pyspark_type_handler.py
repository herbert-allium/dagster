from typing import Mapping

from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from dagster._core.definitions.metadata import RawMetadataValue
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_gcp import build_bigquery_io_manager
from dagster_gcp.bigquery.io_manager import BigQueryClient
from pyspark.sql import DataFrame, SparkSession


def _get_bigquery_write_options(config, table_slice: TableSlice) -> Mapping[str, str]:
    conf = {
        "table": f"{table_slice.database}.{table_slice.schema}.{table_slice.table}",
    }
    if config.get("temporary_gcs_bucket") is not None:
        conf["temporaryGcsBucket"] = config["temporary_gcs_bucket"]
    else:
        conf["writeMethod"] = "direct"
    return conf


class BigQueryPySparkTypeHandler(DbTypeHandler[DataFrame]):
    """Plugin for the BigQuery I/O Manager that can store and load PySpark DataFrames as BigQuery tables.

    Examples:
        .. code-block:: python

            from dagster_gcp import build_bigquery_io_manager
            from dagster_bigquery_pyspark import BigQueryPySparkTypeHandler
            from dagster import asset, Definitions

            @asset(
                key_prefix=["my_dataset"]  # will be used as the dataset in BigQuery
            )
            def my_table():
                ...

            bigquery_io_manager = build_bigquery_io_manager([BigQueryPySparkTypeHandler()])

            defs = Definitions(
                assets=[my_table],
                resources={
                    "io_manager": bigquery_io_manager.configured({
                        "project" : {"env": "GCP_PROJECT"}
                    })
                }
            )

    """

    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: DataFrame, _
    ) -> Mapping[str, RawMetadataValue]:
        options = _get_bigquery_write_options(context.resource_config, table_slice)

        with_uppercase_cols = obj.toDF(*[c.upper() for c in obj.columns])

        with_uppercase_cols.write.format("bigquery").options(**options).mode("append").save()

        return {
            "dataframe_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=field.name, type=field.dataType.typeName())
                        for field in obj.schema.fields
                    ]
                )
            ),
        }

    def load_input(self, context: InputContext, table_slice: TableSlice, _) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("viewsEnabled", "true")
        spark.conf.set("materializationDataset", table_slice.schema)
        df = spark.read.format("bigquery").load(BigQueryClient.get_select_statement(table_slice))

        return df.toDF(*[c.lower() for c in df.columns])

    @property
    def supported_types(self):
        return [DataFrame]


bigquery_pyspark_io_manager = build_bigquery_io_manager(
    [BigQueryPySparkTypeHandler()], default_load_type=DataFrame
)
bigquery_pyspark_io_manager.__doc__ = """
An IO manager definition that reads inputs from and writes PySpark DataFrames to BigQuery.

Returns:
    IOManagerDefinition

Examples:

    .. code-block:: python

        from dagster_bigquery_pyspark import bigquery_pyspark_io_manager
        from dagster import Definitions

        @asset(
            key_prefix=["my_dataset"]  # will be used as the dataset in BigQuery
        )
        def my_table() -> pd.DataFrame:  # the name of the asset will be the table name
            ...

        defs = Definitions(
            assets=[my_table],
            resources={
                "io_manager": bigquery_pyspark_io_manager.configured({
                    "project" : {"env": "GCP_PROJECT"}
                })
            }
        )

    You can tell Dagster in which dataset to create tables by setting the "dataset" configuration value.
    If you do not provide a dataset as configuration to the I/O manager, Dagster will determine a dataset based
    on the assets and ops using the I/O Manager. For assets, the dataset will be determined from the asset key,
    as shown in the above example. The final prefix before the asset name will be used as the dataset. For example,
    if the asset "my_table" had the key prefix ["gcp", "bigquery", "my_dataset"], the dataset "my_dataset" will be
    used. For ops, the dataset can be specified by including a "schema" entry in output metadata. If "schema" is not provided
    via config or on the asset/op, "public" will be used for the dataset.

    .. code-block:: python

        @op(
            out={"my_table": Out(metadata={"schema": "my_dataset"})}
        )
        def make_my_table() -> pd.DataFrame:
            # the returned value will be stored at my_dataset.my_table
            ...

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    .. code-block:: python

        @asset(
            ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
        )
        def my_table_a(my_table: pd.DataFrame) -> pd.DataFrame:
            # my_table will just contain the data from column "a"
            ...

    If you cannot upload a file to your Dagster deployment, or otherwise cannot authenticate with
    GCP via a standard method, (see https://cloud.google.com/docs/authentication/provide-credentials-adc),
    you can provide a service account key as the "gcp_credentials" configuration. Dagster will
    store this key in a temporary file and set GOOGLE_APPLICATION_CREDENTIALS to point to the file.
    After the run completes, the file will be deleted, and GOOGLE_APPLICATION_CREDENTIALS will be
    unset. The key must be base64 encoded to avoid issues with newlines in the keys. You can retrieve
    the base64 encoded with this shell command: cat $GOOGLE_APPLICATION_CREDENTIALS | base64

"""
