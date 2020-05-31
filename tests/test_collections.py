from pathlib import Path, PurePath

import pytest
import pandas as pd

import data_catalog.collections as dc
import data_catalog.datasets as dd
import data_catalog.utils as du


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
            parents = [ParentDataset, ParentCollection]

            def create(self, a, b):
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

    def should_link_item_to_parent_item(self, misc_collection):
        # If the collection has a collection as parent,
        # then an item of the collection must has as parent an item of the
        # parent collection with same key.
        parent_from_parent_collection = misc_collection.get("key_a").parents[1]
        assert parent_from_parent_collection.name() == "ParentCollection:key_a"

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
            keys = {"key_parent_a", "key_parent_b"}

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


@pytest.fixture
def folder_collection():
    class ParentDataset(dd.AbstractDataset):
        parents = []

    class MyCollection(dc.FileCollection):
        relative_path = "datasets"
        keys = du.keys_from_folder("datasets/dir_to_list")

        class Item(dd.FileDataset):
            parents = [ParentDataset]

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
            keys = None
            Item = None

        relative_path = MyCollection.relative_path.as_posix()
        assert relative_path == "MyCollection"

    def should_ensure_relative_path_is_path_object(self):
        class MyCollection(dc.FileCollection):
            keys = None
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
            == "datasets/key_a.dat"
        )

    def should_save_context(self, tmpdir):
        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = dc.FileCollection(context)
        assert a.context == context
