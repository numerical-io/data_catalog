"""Abstract base classes for datasets, filesystems, and collections.

"""
from abc import ABC, ABCMeta
import inspect


class ABCMetaDataset(ABCMeta):
    pass


class ABCMetaCollection(ABCMeta):
    pass


class ABCFileSystem(ABC):
    pass


def is_dataset(x):
    x_class = x if inspect.isclass(x) else type(x)
    return isinstance(x_class, ABCMetaDataset)


def is_collection(x):
    x_class = x if inspect.isclass(x) else type(x)
    return isinstance(x_class, ABCMetaCollection)
