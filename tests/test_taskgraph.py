from pathlib import Path, PurePath

import pytest
import pandas as pd
import dask

import data_catalog.datasets as dd
import data_catalog.collections as dc
import data_catalog.taskgraph as dt
from data_catalog.abc import is_collection


@pytest.fixture
def sample_data_classes():
    """
    """

    class Collection1(dc.FileCollection):
        """A collection with 4 keys
        """

        def keys(self):
            return ["a1", "a2", "b1", "b2"]

        class Item(dd.ParquetDataset):
            def create(self):
                return pd.DataFrame([{self.key: 1}])

    class Collection2(dc.FileCollection):
        """A collection inheriting key-by-key from Collection1
        """

        def keys(self):
            return ["a1", "a2", "b1", "b2"]

        class Item(dd.ParquetDataset):
            parents = [dc.same_key_in(Collection1)]

            def create(self, df):
                return 2 * df

    class Collection3(dc.FileCollection):
        """A collection aggregating Collection1 in two parts
        """

        def keys(self):
            return ["a", "b"]

        class Item(dd.ParquetDataset):
            parents = [
                dc.CollectionFilter(
                    Collection1,
                    lambda self, child: {
                        "a": ["a1", "a2"],
                        "b": ["b1", "b2"],
                    }.get(child, []),
                )
            ]

            def create(self, collection):
                return pd.concat(collection.values(), axis=1)

    class Collection4(dc.FileCollection):
        """An empty collection
        """

        def keys(self):
            return []

        class Item(dd.ParquetDataset):
            def create(self):
                pass

    class Dataset1(dd.ParquetDataset):
        """A dataset inheriting from a collection
        """

        parents = [Collection1]

        def create(self, collection):
            return pd.concat(collection.values(), axis=1)

    class Dataset2(dd.ParquetDataset):
        """A dataset inheriting from an empty collection
        """

        parents = [Collection4]

        def create(self, collection):
            if collection:
                return pd.concat(collection.values(), axis=1)
            else:
                return pd.DataFrame()

    data_classes = {
        "Collection1": Collection1,
        "Collection2": Collection2,
        "Collection3": Collection3,
        "Collection4": Collection4,
        "Dataset1": Dataset1,
        "Dataset2": Dataset2,
    }
    return data_classes


def _obtain_last_update_times(data_objects, context):
    last_update_times = {}
    for data_class in data_objects:
        if is_collection(data_class):
            for k in data_class(context).keys():
                ds = data_class.get(k)(context)
                last_update_times[ds.name()] = ds.last_update_time()
        else:
            ds = data_class(context)
            last_update_times[ds.name()] = ds.last_update_time()
    return last_update_times


class TestCreateDatasets:
    def should_resolve_dependencies(self, sample_data_classes, tmp_path):
        context = {"catalog_uri": tmp_path.absolute().as_uri()}
        dask.get(*dt.create_task_graph(sample_data_classes.values(), context))

        assert sample_data_classes["Collection1"].get("b1")(context).exists()
        col3 = sample_data_classes["Collection3"].get("b")(context).read()
        assert set(col3.columns) == {"b1", "b2"}

    def should_simplify_graph_towards_targets(
        self, sample_data_classes, tmp_path
    ):
        # Create all datasets
        context = {"catalog_uri": tmp_path.absolute().as_uri()}
        dask.get(
            *dt.create_task_graph(
                sample_data_classes.values(),
                context,
                targets=[sample_data_classes["Dataset1"]],
            )
        )

        assert sample_data_classes["Dataset1"](context).exists()
        assert not sample_data_classes["Dataset2"](context).exists()

    def should_not_update_unchanging_datasets(
        self, sample_data_classes, tmp_path
    ):
        # Create all datasets
        context = {"catalog_uri": tmp_path.absolute().as_uri()}
        dask.get(*dt.create_task_graph(sample_data_classes.values(), context))

        # Obtain last update dates of all
        update_times_1 = _obtain_last_update_times(
            sample_data_classes.values(), context
        )

        # Remove one collection member
        sample_data_classes["Collection1"].get("a1")(context).path().unlink()

        # Re-create all datasets
        dask.get(*dt.create_task_graph(sample_data_classes.values(), context))

        # Check that only Collection1:a1, Collection2:a1, Collection3:a, and
        # Dataset1 have changed
        update_times_2 = _obtain_last_update_times(
            sample_data_classes.values(), context
        )
        changed_times = {
            "Collection1:a1",
            "Collection2:a1",
            "Collection3:a",
            "Dataset1",
        }
        for name, time_2 in update_times_2.items():
            if name in changed_times:
                assert time_2 > update_times_1[name]
            else:
                assert time_2 == update_times_1[name]

    def should_handle_empty_collections(self, sample_data_classes, tmp_path):
        # Create all datasets
        context = {"catalog_uri": tmp_path.absolute().as_uri()}
        dask.get(
            *dt.create_task_graph(
                sample_data_classes.values(),
                context,
                targets=[sample_data_classes["Dataset2"]],
            )
        )

        assert sample_data_classes["Dataset2"](context).read().empty

    def should_allow_in_memory_transfer(self, sample_data_classes, tmp_path):
        context = {"catalog_uri": tmp_path.absolute().as_uri()}
        results = dask.get(
            *dt.create_task_graph(
                sample_data_classes.values(),
                context,
                targets=[sample_data_classes["Collection1"].get("a1")],
                in_memory_data_transfer=True,
            )
        )
        assert len(results) == 1
        assert set(results[0].columns) == {"a1"}
