import pytest

from data_catalog.abc import is_dataset, is_collection
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
        key_store = {"key_parent_a", "key_parent_b"}

        class Item(dd.AbstractDataset):
            pass

    return MyCollection


class TestIsDataset:
    def should_recognize_class(self, some_dataset, some_collection):
        assert is_dataset(some_dataset)
        assert not is_dataset(some_collection)

    def should_recognize_instance(self, some_dataset, some_collection):
        assert is_dataset(some_dataset())
        assert not is_dataset(some_collection())

class TestIsCollection:
    def should_recognize_class(self, some_dataset, some_collection):
        assert is_collection(some_collection)
        assert not is_collection(some_dataset)

    def should_recognize_instance(self, some_dataset, some_collection):
        assert is_collection(some_collection())
        assert not is_collection(some_dataset())