from datetime import datetime
from typing import Callable, Optional, Sequence

import pytest
from dagster import (
    AssetKey,
    DagsterUnknownPartitionError,
    IOManager,
    asset,
    materialize,
    materialize_to_memory,
)
from dagster._check import CheckError
from dagster._core.definitions.partition import DynamicPartitionsDefinition, Partition
from dagster._core.test_utils import instance_for_test


@pytest.mark.parametrize(
    argnames=["partition_fn"],
    argvalues=[
        (lambda _current_time: [Partition("a_partition")],),
        (lambda _current_time: [Partition(x) for x in range(10)],),
    ],
)
def test_dynamic_partitions_partitions(
    partition_fn: Callable[[Optional[datetime]], Sequence[Partition]]
):
    partitions = DynamicPartitionsDefinition(partition_fn)

    assert [(p.name, p.value) for p in partitions.get_partitions()] == [
        (p.name, p.value) for p in partition_fn(None)
    ]

    assert partitions.get_partition_keys() == [p.name for p in partition_fn(None)]


@pytest.mark.parametrize(
    argnames=["partition_fn"],
    argvalues=[
        (lambda _current_time: ["a_partition"],),
        (lambda _current_time: [str(x) for x in range(10)],),
    ],
)
def test_dynamic_partitions_keys(partition_fn: Callable[[Optional[datetime]], Sequence[str]]):
    partitions = DynamicPartitionsDefinition(partition_fn)

    assert [(p.name, p.value) for p in partitions.get_partitions()] == [
        (p, p) for p in partition_fn(None)
    ]

    assert partitions.get_partition_keys() == partition_fn(None)


def test_dynamic_partitions_def_methods():
    foo = DynamicPartitionsDefinition(name="foo")
    with instance_for_test() as instance:
        foo.add_partitions(["a", "b"], instance=instance)
        assert set([p.name for p in foo.get_partitions(dynamic_partitions_store=instance)]) == {
            "a",
            "b",
        }
        assert foo.has_partition("a", instance=instance)

        foo.delete_partition("a", instance=instance)
        assert set([p.name for p in foo.get_partitions(dynamic_partitions_store=instance)]) == {"b"}
        assert foo.has_partition("a", instance=instance) is False


def test_dynamic_partitioned_run():
    with instance_for_test() as instance:
        partitions_def = DynamicPartitionsDefinition(name="foo")

        @asset(partitions_def=partitions_def)
        def my_asset():
            return 1

        with pytest.raises(DagsterUnknownPartitionError):
            materialize([my_asset], instance=instance, partition_key="a")

        partitions_def.add_partitions(["a"], instance)
        assert partitions_def.get_partition_keys(dynamic_partitions_store=instance) == ["a"]
        assert materialize([my_asset], instance=instance, partition_key="a").success
        materialization = instance.get_latest_materialization_event(AssetKey("my_asset"))
        assert materialization
        assert materialization.dagster_event.partition == "a"

        with pytest.raises(CheckError):
            partitions_def.get_partition_keys()


def test_dynamic_partitioned_asset_dep():
    partitions_def = DynamicPartitionsDefinition(name="fruits")

    @asset(partitions_def=partitions_def)
    def asset1():
        pass

    @asset(partitions_def=partitions_def, non_argument_deps={"asset1"})
    def asset2(context):
        assert context.partition_key == "apple"
        assert context.asset_key_for_output() == "apple"
        assert context.asset_keys_for_output() == ["apple"]
        assert context.asset_key_for_input() == "apple"
        assert context.asset_keys_for_input() == ["apple"]

    with instance_for_test() as instance:
        partitions_def.add_partitions(["apple"], instance=instance)
        materialize_to_memory([asset1], instance=instance, partition_key="apple")


def test_dynamic_partitioned_asset_io_manager_context():
    partitions_def = DynamicPartitionsDefinition(name="fruits")

    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            assert context.partition_key == "apple"
            assert context.asset_partition_key == "apple"
            assert context.asset_partition_keys == ["apple"]

        def load_input(self, context):
            assert context.partition_key == "apple"
            assert context.asset_partition_key == "apple"
            assert context.asset_partition_keys == ["apple"]

    @asset(partitions_def=partitions_def, io_manager_key="custom_io")
    def asset1():
        return 1

    @asset(
        partitions_def=partitions_def,
        io_manager_key="custom_io",
    )
    def asset2(context, asset1):
        return asset1

    with instance_for_test() as instance:
        partitions_def.add_partitions(["apple"], instance=instance)

        materialize(
            [asset1, asset2],
            instance=instance,
            partition_key="apple",
            resources={"custom_io": MyIOManager()},
        )


def test_dynamic_partitions_no_instance_provided():
    partitions_def = DynamicPartitionsDefinition(name="fruits")

    with pytest.raises(CheckError, match="instance"):
        partitions_def.get_partitions()


def test_dynamic_partitions_mapping():
    partitions_def = DynamicPartitionsDefinition(name="fruits")

    @asset(partitions_def=partitions_def)
    def dynamic1(context):
        assert context.asset_partition_key_for_output() == "apple"
        return 1

    @asset(partitions_def=partitions_def)
    def dynamic2(context, dynamic1):
        assert context.asset_partition_keys_for_input("dynamic1") == ["apple"]
        assert context.asset_partition_key_for_output() == "apple"
        return 1

    @asset
    def unpartitioned(context, dynamic1):
        assert context.asset_partition_keys_for_input("dynamic1") == ["apple"]
        return 1

    with instance_for_test() as instance:
        partitions_def.add_partitions(["apple"], instance=instance)

        materialize([dynamic1, dynamic2, unpartitioned], instance=instance, partition_key="apple")


def test_unpartitioned_downstream_of_dynamic_asset():
    partitions = [
        "apple",
        "banana",
        "cantaloupe",
    ]

    partitions_def = DynamicPartitionsDefinition(name="fruits")

    @asset(partitions_def=partitions_def)
    def dynamic1(context):
        return 1

    @asset
    def unpartitioned(context, dynamic1):
        assert set(context.asset_partition_keys_for_input("dynamic1")) == set(partitions)
        return 1

    with instance_for_test() as instance:
        partitions_def.add_partitions(partitions, instance=instance)

        for partition in partitions[:-1]:
            materialize([dynamic1], instance=instance, partition_key=partition)

        materialize([unpartitioned, dynamic1], instance=instance, partition_key=partitions[-1])
