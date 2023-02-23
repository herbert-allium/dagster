from __future__ import annotations

from enum import Enum
from hashlib import sha256
from typing import TYPE_CHECKING, Callable, Iterator, Mapping, NamedTuple, Optional, Sequence, Union

from typing_extensions import Final

from dagster import _check as check
from dagster._annotations import deprecated
from dagster._utils.cached_method import cached_method

if TYPE_CHECKING:
    from dagster._core.definitions.asset_graph import AssetGraph
    from dagster._core.definitions.events import (
        AssetKey,
        AssetMaterialization,
        AssetObservation,
        Materialization,
    )
    from dagster._core.events.log import EventLogEntry
    from dagster._core.instance import DagsterInstance


class UnknownValue:
    pass


def foo(x):
    return False


UNKNOWN_VALUE: Final[UnknownValue] = UnknownValue()


class DataVersion(
    NamedTuple(
        "_DataVersion",
        [("value", str)],
    )
):
    """(Experimental) Represents a data version for an asset.

    Args:
        value (str): An arbitrary string representing a data version.
    """

    def __new__(
        cls,
        value: str,
    ):
        return super(DataVersion, cls).__new__(
            cls,
            value=check.str_param(value, "value"),
        )


DEFAULT_DATA_VERSION: Final[DataVersion] = DataVersion("INITIAL")
NULL_DATA_VERSION: Final[DataVersion] = DataVersion("NULL")
UNKNOWN_DATA_VERSION: Final[DataVersion] = DataVersion("UNKNOWN")


class DataProvenance(
    NamedTuple(
        "_DataProvenance",
        [
            ("code_version", str),
            ("input_data_versions", Mapping["AssetKey", DataVersion]),
        ],
    )
):
    """(Experimental) Provenance information for an asset materialization.

    Args:
        code_version (str): The code version of the op that generated a materialization.
        input_data_versions (Mapping[AssetKey, DataVersion]): The data versions of the
            inputs used to generate a materialization.
    """

    def __new__(
        cls,
        code_version: str,
        input_data_versions: Mapping["AssetKey", DataVersion],
    ):
        from dagster._core.definitions.events import AssetKey

        return super(DataProvenance, cls).__new__(
            cls,
            code_version=check.str_param(code_version, "code_version"),
            input_data_versions=check.mapping_param(
                input_data_versions,
                "input_data_versions",
                key_type=AssetKey,
                value_type=DataVersion,
            ),
        )

    @staticmethod
    def from_tags(tags: Mapping[str, str]) -> Optional[DataProvenance]:
        from dagster._core.definitions.events import AssetKey

        code_version = tags.get(CODE_VERSION_TAG)
        if code_version is None:
            return None
        input_data_versions = {
            # Everything after the 2nd slash is the asset key
            AssetKey.from_user_string(k.split("/", maxsplit=3)[-1]): DataVersion(tags[k])
            for k, v in tags.items()
            if k.startswith(INPUT_DATA_VERSION_TAG_PREFIX)
            or k.startswith(_OLD_INPUT_DATA_VERSION_TAG_PREFIX)
        }
        return DataProvenance(code_version, input_data_versions)

    @property
    @deprecated
    def input_logical_versions(self) -> Mapping["AssetKey", DataVersion]:
        return self.input_data_versions


# ########################
# ##### TAG KEYS
# ########################

DATA_VERSION_TAG: Final[str] = "dagster/data_version"
_OLD_DATA_VERSION_TAG: Final[str] = "dagster/logical_version"
CODE_VERSION_TAG: Final[str] = "dagster/code_version"
INPUT_DATA_VERSION_TAG_PREFIX: Final[str] = "dagster/input_data_version"
_OLD_INPUT_DATA_VERSION_TAG_PREFIX: Final[str] = "dagster/input_logical_version"
INPUT_EVENT_POINTER_TAG_PREFIX: Final[str] = "dagster/input_event_pointer"


def read_input_data_version_from_tags(
    tags: Mapping[str, str], input_key: "AssetKey"
) -> Optional[DataVersion]:
    value = tags.get(
        get_input_data_version_tag(input_key, prefix=INPUT_DATA_VERSION_TAG_PREFIX)
    ) or tags.get(get_input_data_version_tag(input_key, prefix=_OLD_INPUT_DATA_VERSION_TAG_PREFIX))
    return DataVersion(value) if value is not None else None


def get_input_data_version_tag(
    input_key: "AssetKey", prefix: str = INPUT_DATA_VERSION_TAG_PREFIX
) -> str:
    return f"{prefix}/{input_key.to_user_string()}"


def get_input_event_pointer_tag(input_key: "AssetKey") -> str:
    return f"{INPUT_EVENT_POINTER_TAG_PREFIX}/{input_key.to_user_string()}"


# ########################
# ##### COMPUTE / EXTRACT
# ########################


def compute_logical_data_version(
    code_version: Union[str, UnknownValue],
    input_data_versions: Mapping["AssetKey", DataVersion],
) -> DataVersion:
    """Compute a data version for a value as a hash of input data versions and code version.

    Args:
        code_version (str): The code version of the computation.
        input_data_versions (Mapping[AssetKey, DataVersion]): The data versions of the inputs.

    Returns:
        DataVersion: The computed logical version as a `DataVersion`.
    """
    from dagster._core.definitions.events import AssetKey

    check.inst_param(code_version, "code_version", (str, UnknownValue))
    check.mapping_param(
        input_data_versions, "input_versions", key_type=AssetKey, value_type=DataVersion
    )

    if (
        isinstance(code_version, UnknownValue)
        or UNKNOWN_DATA_VERSION in input_data_versions.values()
    ):
        return UNKNOWN_DATA_VERSION

    ordered_input_versions = [
        input_data_versions[k] for k in sorted(input_data_versions.keys(), key=str)
    ]
    all_inputs = (code_version, *(v.value for v in ordered_input_versions))

    hash_sig = sha256()
    hash_sig.update(bytearray("".join(all_inputs), "utf8"))
    return DataVersion(hash_sig.hexdigest())


def extract_data_version_from_entry(
    entry: EventLogEntry,
) -> Optional[DataVersion]:
    event_data = _extract_event_data_from_entry(entry)
    tags = event_data.tags or {}
    value = tags.get(DATA_VERSION_TAG) or tags.get(_OLD_DATA_VERSION_TAG)
    return None if value is None else DataVersion(value)


def extract_data_provenance_from_entry(
    entry: EventLogEntry,
) -> Optional[DataProvenance]:
    event_data = _extract_event_data_from_entry(entry)
    tags = event_data.tags or {}
    return DataProvenance.from_tags(tags)


def _extract_event_data_from_entry(
    entry: EventLogEntry,
) -> Union["AssetMaterialization", "AssetObservation"]:
    from dagster._core.definitions.events import AssetMaterialization, AssetObservation
    from dagster._core.events import AssetObservationData, StepMaterializationData

    data = check.not_none(entry.dagster_event).event_specific_data
    event_data: Union[Materialization, AssetMaterialization, AssetObservation]
    if isinstance(data, StepMaterializationData):
        event_data = data.materialization
    elif isinstance(data, AssetObservationData):
        event_data = data.asset_observation
    else:
        check.failed(f"Unexpected event type {type(data)}")

    assert isinstance(event_data, (AssetMaterialization, AssetObservation))
    return event_data


# ########################
# ##### STALENESS OPERATIONS
# ########################


class StaleStatus(Enum):
    STALE = "STALE"
    FRESH = "FRESH"
    UNKNOWN = "UNKNOWN"


class StaleStatusCause(NamedTuple):
    status: StaleStatus
    key: AssetKey
    reason: str


class CachingStaleStatusResolver:
    """
    Used to resolve data version information. Avoids redundant database
    calls that would otherwise occur. Intended for use within the scope of a
    single "request" (e.g. GQL request, RunRequest resolution).
    """

    _instance: "DagsterInstance"
    _asset_graph: Optional["AssetGraph"]
    _asset_graph_load_fn: Optional[Callable[[], "AssetGraph"]]

    def __init__(
        self,
        instance: "DagsterInstance",
        asset_graph: Union["AssetGraph", Callable[[], "AssetGraph"]],
    ):
        from dagster._core.definitions.asset_graph import AssetGraph

        self._instance = instance
        if isinstance(asset_graph, AssetGraph):
            self._asset_graph = asset_graph
            self._asset_graph_load_fn = None
        else:
            self._asset_graph = None
            self._asset_graph_load_fn = asset_graph

    def get_status(self, key: AssetKey) -> StaleStatus:
        return self._get_status(key=key)

    def get_status_causes(self, key: AssetKey) -> Sequence[StaleStatusCause]:
        return self._get_status_causes(key=key)

    def get_current_data_version(self, key: AssetKey) -> DataVersion:
        return self._get_current_data_version(key=key)

    def get_projected_data_version(self, key: AssetKey) -> DataVersion:
        return self._get_projected_data_version(key=key)

    @cached_method
    def _get_status(self, key: AssetKey) -> StaleStatus:
        if self.asset_graph.get_partitions_def(key):
            return StaleStatus.UNKNOWN
        else:
            causes = self._get_status_causes(key=key)
            return StaleStatus.FRESH if len(causes) == 0 else causes[0].status

        current_version = self._get_current_data_version(key=key)
        projected_version = self._get_projected_data_version(key=key)
        if projected_version == UNKNOWN_DATA_VERSION:
            return StaleStatus.UNKNOWN
        elif projected_version == current_version:  # always true for source assets
            return StaleStatus.FRESH
        else:
            return StaleStatus.STALE

    @cached_method
    def _get_status_causes(self, key: AssetKey) -> Sequence[StaleStatusCause]:
        current_version = self._get_current_data_version(key=key)
        projected_version = self._get_projected_data_version(key=key)
        if current_version == projected_version:
            return []
        elif current_version == NULL_DATA_VERSION:
            return [StaleStatusCause(StaleStatus.STALE, key, "never materialized")]
        else:
            return list(self._get_stale_status_causes_materialized(key))

    def _get_stale_status_causes_materialized(self, key: AssetKey) -> Iterator[StaleStatusCause]:
        code_version = self.asset_graph.get_code_version(key)
        provenance = self._get_current_data_provenance(key=key)
        proj_dep_keys = self.asset_graph.get_parents(key)

        if provenance:
            prov_versions = provenance.input_data_versions
            all_dep_keys = sorted(set(proj_dep_keys).union(prov_versions.keys()))
            for dep_key in all_dep_keys:
                if dep_key not in prov_versions:
                    yield StaleStatusCause(
                        StaleStatus.STALE, key, f"new input: {dep_key.to_user_string()}"
                    )
                elif dep_key not in proj_dep_keys:
                    yield StaleStatusCause(
                        StaleStatus.STALE, key, f"removed input: {dep_key.to_user_string()}"
                    )
                elif prov_versions[dep_key] != self._get_current_data_version(key=dep_key):
                    yield StaleStatusCause(
                        StaleStatus.STALE, key, f"updated input: {dep_key.to_user_string()}"
                    )
                elif prov_versions[dep_key] != self._get_projected_data_version(key=dep_key):
                    yield StaleStatusCause(
                        StaleStatus.STALE, key, f"stale input: {dep_key.to_user_string()}"
                    )

            if code_version is not None and code_version != provenance.code_version:
                yield StaleStatusCause(StaleStatus.STALE, key, "updated code version")

        if code_version is None:
            yield StaleStatusCause(StaleStatus.UNKNOWN, key, "code version unknown")
        elif provenance is None or provenance.code_version is None:
            yield StaleStatusCause(StaleStatus.UNKNOWN, key, "previous code version unknown")

        for dep_key in proj_dep_keys:
            yield from self._get_status_causes(key=dep_key)

    @property
    def asset_graph(self) -> "AssetGraph":
        if self._asset_graph is None:
            self._asset_graph = check.not_none(self._asset_graph_load_fn)()
        return self._asset_graph

    @cached_method
    def _get_current_data_version(self, *, key: AssetKey) -> DataVersion:
        is_source = self.asset_graph.is_source(key)
        event = self._instance.get_latest_data_version_record(
            key,
            is_source,
        )
        if event is None and is_source:
            return DEFAULT_DATA_VERSION
        elif event is None:
            return NULL_DATA_VERSION
        else:
            data_version = extract_data_version_from_entry(event.event_log_entry)
            return data_version or DEFAULT_DATA_VERSION

    @cached_method
    def _get_projected_data_version(self, *, key: AssetKey) -> DataVersion:
        if self.asset_graph.get_partitions_def(key):
            return UNKNOWN_DATA_VERSION
        elif self.asset_graph.is_source(key):
            event = self._instance.get_latest_data_version_record(key, True)
            if event:
                version = (
                    extract_data_version_from_entry(event.event_log_entry) or DEFAULT_DATA_VERSION
                )
            else:
                version = DEFAULT_DATA_VERSION
        elif self.asset_graph.get_code_version(key) is not None:
            version = self._compute_projected_new_materialization_data_version(key)
        else:
            materialization = self._get_latest_materialization_event(key=key)
            if materialization is None:  # never been materialized
                version = self._compute_projected_new_materialization_data_version(key)
            else:
                current_data_version = self._get_current_data_version(key=key)
                provenance = self._get_current_data_provenance(key=key)
                if (
                    current_data_version is None  # old materialization event before data versions
                    or provenance is None  # should never happen
                    or self._is_provenance_stale(key, provenance)
                ):
                    version = self._compute_projected_new_materialization_data_version(key)
                else:
                    version = current_data_version
        return version

    @cached_method
    def _get_latest_materialization_event(self, *, key: AssetKey) -> Optional[EventLogEntry]:
        return self._instance.get_latest_materialization_event(key)

    @cached_method
    def _get_current_data_provenance(self, *, key: AssetKey) -> Optional[DataProvenance]:
        materialization = self._get_latest_materialization_event(key=key)
        if materialization is None:
            return None
        else:
            return extract_data_provenance_from_entry(materialization)

    # Returns true if the current data version of at least one input asset differs from the
    # recorded data version for that asset in the provenance. This indicates that a new
    # materialization with up-to-date data would produce a different data verson.
    def _is_provenance_stale(self, key: AssetKey, provenance: DataProvenance) -> bool:
        if self._has_updated_dependencies(key=key):
            return True
        else:
            for k, v in provenance.input_data_versions.items():
                if self._get_projected_data_version(key=k) != v:
                    return True
            return False

    @cached_method
    def _has_updated_dependencies(self, *, key: AssetKey) -> bool:
        provenance = self._get_current_data_provenance(key=key)
        if provenance is None:
            return True
        else:
            curr_dep_keys = self.asset_graph.get_parents(key)
            old_dep_keys = set(provenance.input_data_versions.keys())
            return curr_dep_keys != old_dep_keys

    def _compute_projected_new_materialization_data_version(self, key: AssetKey) -> DataVersion:
        dep_keys = self.asset_graph.get_parents(key)
        return compute_logical_data_version(
            self.asset_graph.get_code_version(key) or UNKNOWN_VALUE,
            {dep_key: self._get_projected_data_version(key=dep_key) for dep_key in dep_keys},
        )
