from pathlib import Path, PurePath

import pytest
import pandas as pd

import data_catalog.collections as dc
import data_catalog.datasets as dd
import data_catalog.utils as du
import data_catalog.abc as da


@pytest.fixture
def misc_collection():
    class ParentDataset(dd.AbstractDataset):
        parents = []

    class ParentCollection(dc.AbstractCollection):
        keys = lambda self: ["key_parent_a", "key_parent_b"]

        class Item(dd.AbstractDataset):
            pass

    class MyCollection(dc.AbstractCollection):
        """This is a docstring.
        """

        keys = lambda self: ["key_a", "key_b"]

        class Item(dd.AbstractDataset):
            parents = [
                ParentDataset,
                ParentCollection,
                dc.same_key_in(ParentCollection),
            ]

            def create(self, a, b, c):
                return a

    return MyCollection


class TestAbstractCollection:
    def should_have_mandatory_attributes(self):
        # Missing keys
        with pytest.raises(ValueError):

            class MyCollection(dc.AbstractCollection):
                class Item:
                    pass

        # Missing Item
        with pytest.raises(ValueError):

            class MyCollection(dc.AbstractCollection):
                keys = None

    def should_infer_its_name(self, misc_collection):
        assert misc_collection.name() == "MyCollection"

    def should_infer_its_path_in_catalog(self, misc_collection):
        assert misc_collection.catalog_path() == "test_collections.MyCollection"

    def should_infer_description_from_docstring(self, misc_collection):
        assert misc_collection.description() == "This is a docstring."

    def should_list_keys(self, misc_collection):
        assert misc_collection({}).keys() == ["key_a", "key_b"]

    def should_create_class_for_each_item(self, misc_collection):
        assert issubclass(misc_collection.get("key_a"), misc_collection.Item)

    def should_let_create_multiple_items(self, misc_collection):
        several_items = misc_collection.get(["key_a", "key_b"])
        assert isinstance(several_items, dict)
        assert set(several_items.keys()) == {"key_a", "key_b"}
        assert issubclass(several_items["key_a"], misc_collection.Item)

    # def should_link_item_to_parent_item(self, misc_collection):
    #     # If the collection has a collection as parent,
    #     # then an item of the collection must has as parent an item of the
    #     # parent collection with same key.
    #     parent_from_parent_collection = misc_collection.get("key_a").parents[1]
    #     assert parent_from_parent_collection.name() == "ParentCollection:key_a"

    def should_let_item_inherit_from_collection(self, misc_collection):
        # If the collection has a collection as parent,
        # each item must inherit from the full collection.
        parent_from_item = misc_collection.get("key_a").parents[1]
        assert parent_from_item.name() == "ParentCollection"

    def should_resolve_collection_filters_for_items(self, misc_collection):
        # If the collection has a collection as parent,
        # then an item of the collection must has as parent an item of the
        # parent collection with same key.
        parent_from_item = misc_collection.get("key_a").parents[2]
        assert parent_from_item.name() == "ParentCollection:key_a"

    def should_add_key_attribute_to_item_classes(self, misc_collection):
        assert misc_collection.get("key_a").key == "key_a"

    def should_derive_item_class_attributes_from_collection(
        self, misc_collection
    ):
        item = misc_collection.get("key_a")
        assert item.__doc__ == misc_collection.__doc__
        assert item._catalog_module == misc_collection._catalog_module
        assert item.name() == misc_collection.name() + ":key_a"
        assert item.catalog_path() == misc_collection.catalog_path() + ":key_a"

    def should_have_same_class_and_object_hash(self, misc_collection):
        assert hash(misc_collection) == hash("test_collections.MyCollection")
        assert hash(misc_collection({})) == hash(
            "test_collections.MyCollection"
        )

    def should_be_singleton_class(self, misc_collection):
        class OtherCollection(dc.AbstractCollection):
            def keys(self):
                return ["key_parent_a", "key_parent_b"]

            class Item(dd.AbstractDataset):
                pass

        # All instances are identical
        assert misc_collection({}) == misc_collection({})

        # Instance and class are identical (!)
        assert misc_collection({}) == misc_collection

        # Different classes / instance are not equal
        assert misc_collection != OtherCollection
        assert misc_collection != OtherCollection({})

        # Equality also holds for collection items
        assert misc_collection.get("a") == misc_collection({}).get("a")
        assert misc_collection.get("a") != misc_collection({}).get("b")
        assert misc_collection.get("a") != OtherCollection.get("a")

    def should_save_context(self, misc_collection):
        context = {"a": 1, "b": 2}
        a = misc_collection(context)
        assert a.context == context

    def should_allow_inheritance(self, misc_collection):
        collection_copy = type(
            misc_collection.__name__,
            (misc_collection,),
            {
                "Item": misc_collection.Item,
                "keys": misc_collection.keys,
                "_catalog_module": misc_collection._catalog_module,
            },
        )
        assert collection_copy.catalog_path() == misc_collection.catalog_path()

    def should_validate_keys_method(self):
        with pytest.raises(Exception):

            class SomeCollection(dc.AbstractCollection):
                keys = lambda: ["a", "b"]

                class Item(dd.AbstractDataset):
                    pass


class TestValidateKeysMethod:
    def should_be_callable(self):
        with pytest.raises(TypeError):
            dc._validate_keys_method("some_non_callable")

    def should_have_single_argument(self):
        with pytest.raises(ValueError):
            dc._validate_keys_method(lambda: 3)


@pytest.fixture
def folder_collection():
    class ParentDataset(dd.AbstractDataset):
        parents = []

    class MyCollection(dc.FileCollection):
        relative_path = "datasets/dir_to_list"
        keys = du.keys_from_folder("datasets/dir_to_list")

        class Item(dd.CsvDataset):
            parents = [ParentDataset]
            file_extension = "dat"

            def create(self, x):
                return x

    return MyCollection


class TestFileCollection:
    def should_list_keys_from_folder(self, folder_collection):
        datasets_path = Path(__file__).parent / "examples"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}
        assert folder_collection(context).keys() == {"file_a", "file_b"}

    def should_infer_missing_relative_path(self, tmpdir):
        class MyCollection(dc.FileCollection):
            keys = lambda self: []
            Item = None

        relative_path = MyCollection.relative_path.as_posix()
        assert relative_path == "MyCollection"

    def should_ensure_relative_path_is_path_object(self):
        class MyCollection(dc.FileCollection):
            keys = lambda self: []
            relative_path = "test_datasets/MyCollection"
            Item = None

        assert isinstance(MyCollection.relative_path, PurePath)
        assert MyCollection.relative_path == PurePath(
            "test_datasets/MyCollection"
        )

    def should_set_relative_path_of_derived_item_classes(
        self, folder_collection
    ):
        assert (
            folder_collection.get("key_a").relative_path.as_posix()
            == "datasets/dir_to_list/key_a.dat"
        )

    def should_save_context(self, tmpdir):
        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = dc.FileCollection(context)
        assert a.context == context

    def should_let_read_datasets(self, folder_collection):
        datasets_path = Path(__file__).parent / "examples"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        all_dfs = folder_collection(context).read()
        assert all_dfs.keys() == {"file_a", "file_b"}
        assert all_dfs["file_b"].shape == (2, 3)
        assert all_dfs["file_b"].columns.tolist() == ["a", "b", "c"]

        df_a = folder_collection(context).read(["file_a"])
        assert df_a.keys() == {"file_a"}


@pytest.fixture
def collection_to_filter():
    class MyCollection(dc.FileCollection):
        def keys(self):
            return ["a1", "a2", "b1"]

        class Item(dd.FileDataset):
            parents = []

            def create(self):
                return pd.DataFrame([{"c": 2}, {"c": 4}])

    return MyCollection


@pytest.fixture
def filtered_collection(collection_to_filter):
    def key_filter(self, child_key):
        if child_key == "a":
            return ["a1", "a2"]
        else:
            return ["b1"]

    my_filter = dc.CollectionFilter(collection_to_filter, key_filter)
    filtered_collection = my_filter.filter_by("a")
    return filtered_collection


class TestCollectionFilter:
    def should_create_filtered_collection(
        self, collection_to_filter, filtered_collection
    ):

        # Build a context, with catalog URI pointing to any folder (not used)
        datasets_path = Path(__file__).parent / "examples"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        assert set(filtered_collection(context).keys()) == {"a1", "a2"}
        assert filtered_collection.__doc__ == collection_to_filter.__doc__

        # Check name
        original_name, suffix = filtered_collection.name().split(":")
        assert original_name == collection_to_filter.name()
        assert suffix.startswith("filter")

        # Check catalog path
        assert (
            filtered_collection._catalog_module
            == collection_to_filter._catalog_module
        )
        assert (
            filtered_collection.catalog_path()
            == collection_to_filter.catalog_path() + ":" + suffix
        )

    def should_make_filtered_collection_distinct_from_original(
        self, collection_to_filter, filtered_collection
    ):
        assert filtered_collection != collection_to_filter
        assert (
            filtered_collection.catalog_path()
            != collection_to_filter.catalog_path()
        )

    def should_create_same_datasets_as_original_collection(
        self, collection_to_filter, filtered_collection
    ):
        ds_from_filtered = filtered_collection.get("a1")
        ds_from_original = collection_to_filter.get("a1")

        assert ds_from_filtered == ds_from_original
        assert issubclass(ds_from_filtered, collection_to_filter.Item)
        assert ds_from_filtered.name() == ds_from_original.name()
        assert (
            ds_from_filtered.catalog_path() == ds_from_original.catalog_path()
        )
        assert ds_from_filtered.relative_path == ds_from_original.relative_path

    def should_let_filter_access_the_original_collection(
        self, collection_to_filter
    ):
        # Build a context, with catalog URI pointing to any folder (not used)
        datasets_path = Path(__file__).parent / "examples"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        # Check that we can access e.g. the keys of the original collection
        def key_filter(self, child_key):
            original_keys = super(self.__class__, self).keys()
            return original_keys

        my_filter = dc.CollectionFilter(collection_to_filter, key_filter)
        filtered_collection = my_filter.filter_by("a")
        assert (
            filtered_collection(context).keys()
            == collection_to_filter(context).keys()
        )
        filtered_collection(context).keys()


class TestSingleDatasetFilter:
    def should_return_single_dataset(self, collection_to_filter):
        my_filter = dc.SingleDatasetFilter(
            collection_to_filter, lambda key: key
        )
        dataset = my_filter.filter_by("b1")

        assert dataset.key == "b1"

    def should_be_recognized_as_filter(self, collection_to_filter):
        my_filter = dc.SingleDatasetFilter(
            collection_to_filter, lambda key: key
        )
        assert da.is_collection_filter(my_filter)


class TestSameKeyIn:
    def should_return_single_dataset(self, collection_to_filter):
        my_filter = dc.same_key_in(collection_to_filter)
        dataset = my_filter.filter_by("b1")

        assert dataset.key == "b1"
