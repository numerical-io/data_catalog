import pytest

from data_catalog.abc import (
    is_dataset,
    is_collection,
    is_collection_filter,
)
import data_catalog.datasets as dd
import data_catalog.collections as dc


@pytest.fixture
def some_dataset():
    class MyDataset(dd.AbstractDataset):
        pass

    return MyDataset


@pytest.fixture
def some_collection():
    class MyCollection(dc.AbstractCollection):
        keys = lambda self: ["key_parent_a", "key_parent_b"]

        class Item(dd.AbstractDataset):
            pass

    return MyCollection


class TestIsDataset:
    def should_recognize_class(self, some_dataset, some_collection):
        assert is_dataset(some_dataset)
        assert not is_dataset(some_collection)

    def should_recognize_instance(self, some_dataset, some_collection):
        context = {}
        assert is_dataset(some_dataset(context))
        assert not is_dataset(some_collection(context))


class TestIsCollection:
    def should_recognize_class(self, some_dataset, some_collection):
        assert is_collection(some_collection)
        assert not is_collection(some_dataset)

    def should_recognize_instance(self, some_dataset, some_collection):
        context = {}
        assert is_collection(some_collection(context))
        assert not is_collection(some_dataset(context))


class TestIsCollectionFilter:
    def should_recognize_instance(self, some_collection):
        context = {}
        my_filter = dc.CollectionFilter(
            some_collection, lambda a, b: True
        )
        assert is_collection_filter(my_filter)
        assert not is_collection_filter(some_collection)
        assert not is_collection_filter(some_collection(context))
