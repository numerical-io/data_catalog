"""Dataset structures to define sources and data transformations.

Concrete classes are: CsvDataset, ParquetDataset, PickleDataset.
They all inherit from FileDataset, a class to represent all datasets saved as
a file on a disk.
"""
import os
from pathlib import PurePath
import pickle
import inspect

import pandas as pd

from .file_systems import AbstractFileSystem, LocalFileSystem, S3FileSystem


DATASET_CATEGORIES = {
    'raw': 'raw',
    'cln': 'clean',
    'bse': 'base',
    'ftr': 'features',
    'inp': 'inputs',
    'mdl': 'models',
    'res': 'results',
    }


class AbstractDataset:
    registry = []

    def __init__(self, name, description=''):
        self._name = _validate_name(name)
        self.description = description
        AbstractDataset.registry.append(self)

    @property
    def name(self):
        return self._name

    @property
    def category(self):
        return _retrieve_category(self._name)


class FileDataset(AbstractDataset):
    """A dataset saved as a file on a disk.

    This class is abstract.

    A dataset can be created either with the constructor, or with the factory
    method `from_parents`. In general, the first form is more convenient when
    the file is defined from an existing file on disk, while the second form is
    only applicable to datasets created from parent datasets.

    Attributes:
        filesystem (AbstractFileSystem-like): filesystem on which the file
            resides. This is a class attribute, shared by all objects.
        file_extension (str): file extension for files of this type.
        is_binary_file (bool): True if file must be opened as binary.
    """

    file_system = AbstractFileSystem()
    file_extension = 'dat'
    is_binary_file = True

    def __init__(self, name, description='', relpath=None, create=None,
            parents=None, read_kwargs=None, write_kwargs=None):
        """Create a dataset saved as a file on a disk.

        A few remarks:
        - Either `create` or `relpath` must be set.
        - When `create` is set, parents will be inferred from the names of
            the `create` function, unless an explicit `parents` argument is set.
        - if `relpath` is not set, it is inferred from the dataset name and
            layer.

        Args:
            name (str): name of the dataset. Must be a valid name (starting
                with the 3-letter code of a data layer).
            description (str): description of the dataset.
            relpath (str): path of the dataset, relative from the root path of
                the `file_system` attribute.
            create (callable): when the dataset is created from parent datasets,
                function to create it from its parents.
            parents (list of str): names of the parent datasets, if applicable.
            read_kwargs (dict): dictionary of arguments to pass when reading
                the file on disk.
            read_kwargs (dict): dictionary of arguments to pass when writing
                the file on disk.
        """

        if (relpath is None) and (create is None):
            raise ValueError('Either relpath or create argument must be set.')

        super().__init__(name, description)
        self._create = create
        self._parents = _infer_parents(create, parents)
        self._read_kwargs = {} if read_kwargs is None else read_kwargs
        self._write_kwargs = {} if write_kwargs is None else write_kwargs

        if relpath:
            self._relpath = PurePath(relpath)
        else:
            self._relpath = (
                PurePath(self.category)/f'{self._name}.{self.file_extension}')

    def read(self):
        """Read the dataset on disk.

        Returns:
            pandas.DataFrame
        """
        with self.file_system.open(self._relpath, self.read_mode) as file:
            return self._read(file, **self._read_kwargs)

    def write(self, df):
        """Read the dataset to disk.

        Args:
            df (pandas.DataFrame): dataset, to write on disk.
        """
        with self.file_system.open(self._relpath, self.write_mode) as file:
            return self._write(df, file, **self._write_kwargs)

    def _read(self, file, **kwargs):
        raise NotImplementedError('Abstract file dataset.')

    def _write(self, df, file, **kwargs):
        raise NotImplementedError('Abstract file dataset.')

    def create(self, *args):
        """Create the dataset from its parents.
        """
        return self._create(*args)

    @property
    def parents(self):
        """List of the names of this dataset's parents.
        """
        return self._parents

    @property
    def path(self):
        """Full path on disk of this dataset.
        """
        return self.file_system.full_path(self._relpath)

    def last_update_time(self):
        """Return the last update time of this dataset on disk.

        Returns:
            datetime-like or number, type depends on the file system.
        """
        return self.file_system.last_update_time(self._relpath)

    def exists(self):
        """Retrun whether the dataset exists.
        """
        return self.file_system.exists(self._relpath)

    @property
    def read_mode(self):
        """Read mode of the dataset, can be binary or text.
        """
        return 'rb' if self.is_binary_file else 'r'

    @property
    def write_mode(self):
        """Write mode of the dataset, can be binary or text.
        """
        return 'wb' if self.is_binary_file else 'w'

    @classmethod
    def from_parents(cls, description='', name=None, parents=None):
        """Define a dataset based on the function to create if from its parents.

        Dataset name and parents are inferred from the function name and
        argument names, unless they are provided explicitely.

        Args:
            description (str): description of the dataset.
            name (str): name of the dataset. Must be a valid name (starting
                with the 3-letter code of a data layer).
            parents (list of str): names of the parent datasets, if applicable.
        """
        name = name
        parents = parents
        description = description
        def decorator(func):
            # Create dataset object, and return decorated function untouched
            ds_name = name if name else func.__name__
            cls(ds_name, description, create=func, parents=parents)
            return func
        return decorator


class CsvDataset(FileDataset):
    """A CSV dataset saved as a file on a disk.
    """
    file_extension = 'csv'
    is_binary_file = False

    def _read(self, file, **kwargs):
        return pd.read_csv(file, **kwargs)

    def _write(self, df, file, **kwargs):
        df.to_csv(file, **kwargs)


class ParquetDataset(FileDataset):
    """A Parquet dataset saved as a file on a disk.
    """
    file_extension = 'parquet'
    is_binary_file = True

    def _read(self, file, **kwargs):
        return pd.read_parquet(file, **kwargs)

    def _write(self, df, file, **kwargs):
        df.to_parquet(file, **kwargs)


class PickleDataset(FileDataset):
    """A Pickle dataset saved as a file on a disk.
    """
    file_extension = 'pickle'
    is_binary_file = True

    def _read(self, file, **kwargs):
        return pickle.load(file, **kwargs)

    def _write(self, df, file, **kwargs):
        pickle.dump(df, file, **kwargs)


def _find_mandatory_arguments(func):
    """Find mandatory arguments of a function (those without default value).
    """
    signature = inspect.signature(func)
    mandatory_arguments = [
        param.name for param in signature.parameters.values()
        if param.default is param.empty
    ]
    return mandatory_arguments


def _infer_parents(create, parents):
    if create is None:
        if parents is None:
            return None
        else:
            raise ValueError(
                'Argument parents cannot be set if argument create is not set.')

    else:
        parents_inferred_from_create = _find_mandatory_arguments(create)
        if parents is None:
            return parents_inferred_from_create
        else:
            from_parents = len(parents)
            from_create = len(parents_inferred_from_create)
            if from_parents != from_create:
                raise ValueError(
                    f'Number of arguments of create ({from_create}) '
                    'does not match number of parents ({from_parents}).')
            return parents


def _validate_name(name):
    """Validate dataset name, which must start with a valid category identifier.
    """
    _retrieve_category(name)
    return name


def _validate_category(cat):
    """Validate the category of a dataset.
    """
    # Translate to long-form name, if needed
    cat = DATASET_CATEGORIES.get(cat, cat)

    if cat in DATASET_CATEGORIES.values():
        return cat
    else:
        msg = (
            'Dataset category {} does not exist. '.format(cat)
            + 'Category must be one of {}.'.format([
                name for items in DATASET_CATEGORIES.items() for name in items
            ]))
        raise ValueError(msg)


def _retrieve_category(name):
    """Retrieve a dataset category from a dataset name.

    The category is the substring until the first underscore.
    """
    category = name[:name.find('_')]
    return _validate_category(category)
