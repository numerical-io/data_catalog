"""Collections of datasets.

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
    """Metaclass for collection classes.

    This metaclass ensures the class is properly defined, with valid attributes.
    """

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
        """Equality operator for collections.

        Note that an object and its class are considered equal.
        """
        if is_dataset(other) or is_collection(other):
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplementedError()

    def __repr__(self):
        return self.catalog_path()


def _validate_keys_method(keys):
    """Check that the keys method is valid.
    """
    if callable(keys):
        num_args = len(inspect.signature(keys).parameters)
        if num_args != 1:
            raise ValueError(
                "The keys method must have a single argument (self)."
            )
    else:
        raise TypeError("The keys attribute must be a callable.")


class AbstractCollection(metaclass=MetaCollection):
    """Abstract class for collections.

    Inheriting classes must define the following attributes:
    - a `keys` method, with a single argument `self`, returning a list of the
      collection keys.
    - an `Item` nested class, inheriting from a dataset class.
    """

    def keys(self):
        pass

    class Item:
        pass

    def __init__(self, context):
        """Set the collection context.

        Args:
            context (dict): key-value parameters defining the execution context.
        """
        self.context = context

    @classmethod
    def description(cls):
        """Return the description of the collection.

        Returns:
            str: Description of the collection, None if unavailable.
        """
        if cls.__doc__:
            return cls.__doc__.strip()
        else:
            return None

    @classmethod
    def name(cls):
        """Return the name of the collection.

        Returns:
            str: Name of the collection (class name).
        """
        return cls.__name__

    @classmethod
    def catalog_path(cls):
        """Return the catalog path of the collection.

        Returns:
            str: The catalog path. It is made of the path in the module/package,
              and of the class name, separated by periods. It is a unique
              identifier of the class.
        """
        return f"{cls._catalog_module}.{cls.__name__}"

    @classmethod
    def get(cls, key):
        """Get one or several datasets from the collection.

        Dataset classes created from a collection have their key saved as the
        `key` attribute.

        Args:
            key (str or list of str): The key(s) for which the dataset classes
              must be returned.

        Returns:
            dataset class, or dict of dataset classes: If a single key is
              requested, the corresponding dataset class; otherwise a dict
              indexed by the requested keys, with dataset classes as values.
        """
        if isinstance(key, list) or isinstance(key, set):
            return {k: cls.get(k) for k in key}

        attributes = cls._set_item_attributes(cls, key)
        base_name = cls.name().split(":")[0]
        item_cls = type(f"{base_name}:{key}", (cls.Item,), attributes)
        return item_cls

    @staticmethod
    def _set_item_attributes(cls, key):
        """Set class attributes to create a dataset class from the Item class.
        """
        parents = [
            parent.filter_by(key) if is_collection_filter(parent) else parent
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
        """Equality operator for collections.

        Note that an object and its class are considered equal.
        """
        if is_dataset(other) or is_collection(other):
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplementedError()

    def __repr__(self):
        return self.catalog_path() + "(context)"

    def read(self, keys=None):
        """Read a collection or a subset of it.

        Args:
            keys (list of str): Keys to read. If None, all keys are read.

        Returns:
            dict: The data from requested collection items, indexed by key.
        """
        raise NotImplementedError()


class MetaFileCollection(MetaCollection):
    """Metaclass for collection classes containing file datasets.

    This metaclass ensures the class is properly defined, with valid attributes.
    """

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
    """Collection of which items are FileDatasets.

    Inheriting classes must have the same attributes `keys` and `Item` as
    AbstractCollection.
    """

    def keys(self):
        pass

    class Item:
        pass

    def __init__(self, context):
        """Sets the collection context and instanciate the file system.

        Args:
            context (dict): key-value parameters defining the execution context.
              The context must contain a key `catalog_uri` defining the
              location of catalog data files on disk (str starting with
              `file://`` or `s3://`). It may also contain a key `fs_kwargs`
              with keyword arguments passed on to the filesystem object (e.g.
              additional arguments for authentication or configuration).
        """
        uri = context["catalog_uri"]
        kwargs = context.get("fs_kwargs", {})
        self.file_system = create_filesystem_from_uri(uri, **kwargs)
        super().__init__(context)

    @staticmethod
    def _set_item_attributes(cls, key):
        """Set class attributes to create a dataset class from the Item class.

        Among others, define the item `relative_path` attribute from the
        collection `relative_path`.
        """

        attributes = super()._set_item_attributes(cls, key)
        attributes["relative_path"] = str(
            PurePath(cls.relative_path) / f"{key}.{cls.Item.file_extension}"
        )
        return attributes

    def read(self, keys=None):
        """Read a collection or a subset of it.

        Args:
            keys (list of str): Keys to read. If None, all keys are read.

        Returns:
            dict: The data from requested collection items, indexed by key.
        """
        if keys is None:
            keys = self.keys()

        all_dfs = {key: self.get(key)(self.context).read() for key in keys}
        return all_dfs


class CollectionFilter(ABCCollectionFilter):
    """A filter to create a collection as subset from another collection.

    """

    def __init__(self, collection, key_filter):
        """Initialize the collection filter.

        Args:
            collection (AbstractCollection): The collection to select an item
              from.
            key_filter (callable): callable taking as inputs a
              collection and a child key, and returning a list of keys. The
              returned keys must be a subset of the keys in collection; this
              subset changes as a function of child key.
        """
        self.collection = collection
        self.key_filter = key_filter

    def filter_by(self, child_key):
        """Create a subset of a collection.

        The subset changes as a function of the `child_key` value.

        Args:
            child_key (str): key used to define the subset.
        """

        # Define the .keys() method for the filtered collection
        def keys(collection_self):
            return self.key_filter(collection_self, child_key)

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
    """A collection filter that will return a single element.

    """

    def __init__(self, collection, key_filter):
        """Initialize the collection filter.

        Args:
            collection (AbstractCollection): The collection to select an item
              from.
            key_filter (callable): callable taking as input a single key (from
              the child collection), and returning the single corresponding key
              from the parent collection.
        """
        self.collection = collection
        self.key_filter = key_filter

    def filter_by(self, child_key):
        """Return a single dataset from the collection.

        Args:
            child_key (str): The key of the requested collection item.
        """
        key = self.key_filter(child_key)
        return self.collection.get(key)


def same_key_in(collection):
    """Creates a collection filter that will return a single element.

    Shortcut to create a SingleDatasetFilter.

    Args:
        collection (AbstractCollection): The collection to select an item from.

    Returns:
        SingleDatasetFilter.
    """
    return SingleDatasetFilter(collection, lambda key: key)
