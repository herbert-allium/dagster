def get_iris_data_for_date(*args, **kwargs):
    pass


# start_example

import pandas as pd

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionDefinition,
    asset,
)


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2023-01-01"),
            "species": StaticPartitionDefinition(
                ["Iris-setosa", "Iris-virginica", "Iris-versicolor"]
            ),
        }
    ),
    metadata={"partition_expr": {"date": "TIMESTAMP_SECONDS(time::INT)", "species": "SPECIES"}},
)
def iris_data_partitioned(context) -> pd.DataFrame:
    partition = partition = context.partition_key.keys_by_dimension
    species = partition["species"]
    date = partition["date"]

    # get_iris_data_for_date fetches all of the iris data for a given date,
    # the returned dataframe contains a column named 'time' with that stores
    # the time of the row as an integer of seconds since epoch
    full_df = get_iris_data_for_date(date)

    return full_df[full_df["Species"] == species]


@asset
def iris_cleaned(iris_data_partitioned: pd.DataFrame):
    return iris_data_partitioned.dropna().drop_duplicates()


# end_example
