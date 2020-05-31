"""Dataset objects defining data representation, and relationships.

"""
import inspect
from pathlib import PurePath
import pickle

import pandas as pd

from .abc import ABCMetaDataset, ABCMetaCollection, is_dataset, is_collection
from .file_systems import (
    LocalFileSystem,
    S3FileSystem,
    create_filesystem_from_uri,
)
from .utils import _find_mandatory_arguments


class MetaDataset(ABCMetaDataset):
    def __new__(mcs, name, bases, attrs, **kwargs):

        # Set default values for class attributes
        if "parents" not in attrs:
            attrs["parents"] = []
        if "create" not in attrs:
            attrs["create"] = None

        # Check compatibility of "parents" and "create"
        if attrs["create"] is not None:
            create_args = _find_mandatory_arguments(attrs["create"])
            # -1 to deduct `self`
            num_create_args = len(create_args) - 1
        else:
            num_create_args = 0

        num_parents = len(attrs["parents"])

        if num_create_args != num_parents:
            raise ValueError(
                "The `create` function is incompatible with `parents`."
                f" `create` has {num_create_args} args (self excluded)"
                f" while `parents` has length {num_parents}."
            )

        # Check that parents are datasets or collections
        for parent in attrs["parents"]:
            is_dataset = isinstance(parent, ABCMetaDataset)
            is_collection = isinstance(parent, ABCMetaCollection)
            if not (is_dataset or is_collection):
                msg = "The items in `parents` must be datasets or collections."
                raise ValueError(msg)

        # Set path in catalog from module path, if not set
        if "_catalog_module" not in attrs:
            attrs["_catalog_module"] = attrs['__module__']

        return super().__new__(mcs, name, bases, attrs)

    def __hash__(self):
        return hash(self.catalog_path())

    def __eq__(self, other):
        if is_dataset(other) or is_collection(other):
            print(type(self), type(other))
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplemented()


class AbstractDataset(metaclass=MetaDataset):
    """Doc AbstractDataset
    """

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

    def __hash__(self):
        return hash(self.catalog_path())

    def __eq__(self, other):
        if is_dataset(other) or is_collection(other):
            print(type(self), type(other))
            return self.catalog_path() == other.catalog_path()

        else:
            raise NotImplemented()


class MetaFileDataset(MetaDataset):
    def __new__(mcs, name, bases, attrs, **kwargs):

        # Ensure relative path is PurePath object
        if "relative_path" in attrs:
            attrs["relative_path"] = PurePath(attrs["relative_path"])

        cls = super().__new__(mcs, name, bases, attrs, **kwargs)

        # We do not let relative_path be inherited from other objects.
        # Therefore, if it was missing in attrs, we override its value here.
        if "relative_path" not in attrs:
            dirpath = PurePath("/".join(cls._catalog_module.split(".")[1:]))
            file_extension = getattr(cls, "file_extension")
            filename = f"{name}.{file_extension}"
            setattr(cls, "relative_path", dirpath / filename)

        return cls


class FileDataset(AbstractDataset, metaclass=MetaFileDataset):

    file_extension = "dat"
    is_binary_file = True
    read_kwargs = {}
    write_kwargs = {}

    def __init__(self, context):
        uri = context["catalog_uri"]
        kwargs = context.get("fs_kwargs", {})
        self.file_system = create_filesystem_from_uri(uri, **kwargs)
        super().__init__(context)

    def read(self):
        """Read the dataset on disk.

        Returns:
            pandas.DataFrame
        """
        open_kwargs = {}
        if (not self.is_binary_file) & ("encoding" in self.read_kwargs):
            open_kwargs["encoding"] = self.read_kwargs["encoding"]

        with self.file_system.open(
            self.relative_path, self.read_mode(), **open_kwargs
        ) as file:
            return self._read(file, **self.read_kwargs)

    def write(self, df):
        """Write the dataset to disk.

        Args:
            df (pandas.DataFrame): dataset, to write on disk.
        """
        open_kwargs = {}
        if (not self.is_binary_file) & ("encoding" in self.write_kwargs):
            open_kwargs["encoding"] = self.write_kwargs["encoding"]

        with self.file_system.open(
            self.relative_path, self.write_mode(), **open_kwargs
        ) as file:
            return self._write(df, file, **self.write_kwargs)

    def _read(self, file, **kwargs):
        raise NotImplementedError("Abstract file dataset.")

    def _write(self, df, file, **kwargs):
        raise NotImplementedError("Abstract file dataset.")

    def path(self):
        """Full path on disk of this dataset.
        """
        return self.file_system.full_path(self.relative_path)

    def last_update_time(self):
        """Return the last update time of this dataset on disk.

        Returns:
            datetime-like or number, type depends on the file system.
        """
        return self.file_system.last_update_time(self.relative_path)

    def exists(self):
        """Return whether the dataset exists.
        """
        return self.file_system.exists(self.relative_path)

    @classmethod
    def read_mode(cls):
        """Read mode of the dataset, can be binary or text.
        """
        return "rb" if cls.is_binary_file else "r"

    @classmethod
    def write_mode(cls):
        """Write mode of the dataset, can be binary or text.
        """
        return "wb" if cls.is_binary_file else "w"


class CsvDataset(FileDataset):
    """A CSV dataset saved as a file on a disk.
    """

    file_extension = "csv"
    is_binary_file = False

    def _read(self, file, **kwargs):
        return pd.read_csv(file, **kwargs)

    def _write(self, df, file, **kwargs):
        df.to_csv(file, **kwargs)


class ParquetDataset(FileDataset):
    """A Parquet dataset saved as a file on a disk.
    """

    file_extension = "parquet"
    is_binary_file = True

    def _read(self, file, **kwargs):
        return pd.read_parquet(file, **kwargs)

    def _write(self, df, file, **kwargs):
        df.to_parquet(file, **kwargs)


class PickleDataset(FileDataset):
    """A Pickle dataset saved as a file on a disk.
    """

    file_extension = "pickle"
    is_binary_file = True

    def _read(self, file, **kwargs):
        return pickle.load(file, **kwargs)

    def _write(self, df, file, **kwargs):
        pickle.dump(df, file, **kwargs)


class ExcelDataset(FileDataset):
    """An Excel dataset saved as a file on a disk.
    """

    file_extension = "xlsx"
    is_binary_file = True

    def _read(self, file, **kwargs):
        return pd.read_excel(file, **kwargs)

    def _write(self, df, file, **kwargs):
        df.to_excel(file, **kwargs)
