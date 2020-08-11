"""Collections or datasets.

"""
from pathlib import PurePath

from .abc import ABCMetaCollection, is_dataset, is_collection
from .file_systems import create_filesystem_from_uri


class MetaCollection(ABCMetaCollection):
    def __new__(mcs, name, bases, attrs, **kwargs):

        # Check presence of mandatory attributes
        mandatory_attributes = {"keys", "Item"}
        missing_attributes = mandatory_attributes.difference(attrs)
        if missing_attributes:
            msg = f"These attributes are missing: {missing_attributes}."
            raise ValueError(msg)

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


class AbstractCollection(metaclass=MetaCollection):

    keys = None

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
        item_cls = type(f"{cls.name()}:{key}", (cls.Item,), attributes)
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

    keys = None

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
