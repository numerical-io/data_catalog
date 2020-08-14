"""Collections or datasets.

"""
from pathlib import PurePath
import uuid
import inspect

from .abc import (
    ABCMetaCollection,
    ABCCollectionFilter,
    is_dataset,
    is_collection,
    is_collection_filter,
)
from .file_systems import create_filesystem_from_uri


class MetaCollection(ABCMetaCollection):
    def __new__(mcs, name, bases, attrs, **kwargs):

        # Check presence of mandatory attributes
        mandatory_attributes = {"keys", "Item"}
        missing_attributes = mandatory_attributes.difference(attrs)
        if missing_attributes:
            msg = f"These attributes are missing: {missing_attributes}."
            raise ValueError(msg)

        # Validate the keys attribute
        _validate_keys_method(attrs["keys"])

        # Set path in catalog from module path, if not set
        if "_catalog_module" not in attrs:
            attrs["_catalog_module"] = attrs["__module__"]

        return super().__new__(mcs, name, bases, attrs)

    def __hash__(self):
        return hash(self.catalog_path())

    def __eq__(self, other):
        if is_dataset(other) or is_collection(other):
            print(type(self), type(other))
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplemented()


def _validate_keys_method(keys):
    if callable(keys):
        num_args = len(inspect.signature(keys).parameters)
        if num_args != 1:
            raise ValueError(
                "The keys method must have a single argument (self)."
            )
    else:
        raise TypeError("The keys attribute must be a callable.")


class AbstractCollection(metaclass=MetaCollection):

    def keys(self):
        pass

    class Item:
        pass

    def __init__(self, context):
        self.context = context

    @classmethod
    def description(cls):
        if cls.__doc__:
            return cls.__doc__.strip()
        else:
            return None

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def catalog_path(cls):
        return f"{cls._catalog_module}.{cls.__name__}"

    @classmethod
    def get(cls, key):
        if isinstance(key, list) or isinstance(key, set):
            return {k: cls.get(k) for k in key}

        attributes = cls._set_item_attributes(cls, key)
        base_name = cls.name().split(":")[0]
        item_cls = type(f"{base_name}:{key}", (cls.Item,), attributes)
        return item_cls

    @staticmethod
    def _set_item_attributes(cls, key):
        parents = [
            parent.get(key) if is_collection(parent) else parent
            for parent in cls.Item.parents
        ]
        attributes = {
            "__doc__": cls.__doc__,
            "_catalog_module": cls._catalog_module,
            "key": key,
            # parents and create must be explicitely set, because they are
            # not inherited (as set in dataset metaclass)
            "parents": parents,
            "create": cls.Item.create,
        }
        return attributes

    def __hash__(self):
        return hash(self.catalog_path())

    def __eq__(self, other):
        if is_dataset(other) or is_collection(other):
            print(type(self), type(other))
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplemented()

    def read(self, keys=None):
        raise NotImplemented()


class MetaFileCollection(MetaCollection):
    def __new__(mcs, name, bases, attrs, **kwargs):
        if "relative_path" in attrs:
            # Ensure relative path is PurePath object
            attrs["relative_path"] = PurePath(attrs["relative_path"])

        cls = super().__new__(mcs, name, bases, attrs, **kwargs)

        # We do not let relative_path be inherited from other objects.
        # Therefore, if it was missing in attrs, we override its value here.
        if "relative_path" not in attrs:
            parent_dirpath = PurePath(
                "/".join(cls._catalog_module.split(".")[1:])
            )
            setattr(cls, "relative_path", parent_dirpath / name)

        return cls


class FileCollection(AbstractCollection, metaclass=MetaFileCollection):

    def keys(self):
        pass

    class Item:
        pass

    def __init__(self, context):
        uri = context["catalog_uri"]
        kwargs = context.get("fs_kwargs", {})
        self.file_system = create_filesystem_from_uri(uri, **kwargs)
        super().__init__(context)

    @staticmethod
    def _set_item_attributes(cls, key):

        attributes = super()._set_item_attributes(cls, key)
        attributes["relative_path"] = str(
            PurePath(cls.relative_path) / f"{key}.{cls.Item.file_extension}"
        )
        return attributes

    def read(self, keys=None):
        if keys is None:
            keys = self.keys()

        all_dfs = {key: self.get(key)(self.context).read() for key in keys}
        return all_dfs


class CollectionFilter(ABCCollectionFilter):
    def __init__(self, collection, func):
        self.collection = collection
        self.func = func

    def filter_by(self, child_key):
        # Define function to filter keys of parent, parametrized by child key
        def filter_func(parent_key):
            return self.func(child_key, parent_key)

        # Define the .keys() method for the filtered collection
        def keys(collection_self):
            original_keys = super(
                collection_self.__class__, collection_self
            ).keys()

            return [key for key in original_keys if filter_func(key)]

        # Create the class for the filtered collection
        # The filtered collection inherits the original collection. Some
        # attributes must be nonetheless specified explicitely, to follow
        # the logic of the metaclass.
        filtered_collection = type(
            self.collection.__name__ + ":filter" + uuid.uuid4().hex,
            (self.collection,),
            {
                "_catalog_module": self.collection._catalog_module,
                "Item": self.collection.Item,
                "keys": keys,
                "__doc__": self.collection.__doc__,
                "relative_path": self.collection.relative_path,
            },
        )
        return filtered_collection


class SingleDatasetFilter(ABCCollectionFilter):
    def __init__(self, collection):
        self.collection = collection

    def filter_by(self, child_key):
        return self.collection.get(child_key)


def same_key_in(collection):
    return SingleDatasetFilter(collection)
