"""Misc functions and classes.

"""
from pathlib import PurePath
import inspect
from functools import partial
import re

from .abc import is_dataset, is_collection


def _find_mandatory_arguments(func):
    """Find mandatory arguments of a function (those without default value).
    """
    signature = inspect.signature(func)
    mandatory_arguments = [
        param.name
        for param in signature.parameters.values()
        if param.default is param.empty
    ]
    return mandatory_arguments


def keys_from_folder(relative_folder_path):
    """

    TBD

    The returned closure will be bound, so must have first argument `self`.
    self must have a file_system attribute.
    Returns a set of file names without extension
    - hidden files are excluded
    - files with same name but different extension appear only once in the list
    - directories are included (should be avoided ?)
    """

    def list_keys(self):
        filenames = self.file_system.listdir(relative_folder_path)
        stems = {PurePath(name).stem for name in filenames}
        return stems

    return list_keys


def is_sub_module(x, module):
    if inspect.ismodule(x):
        return x.__name__.startswith(module.__name__) and (
            len(x.__name__) > len(module.__name__)
        )
    return False


def is_member_class(x, module):
    if inspect.isclass(x):
        return x.__module__ == module.__name__
    return False


def list_catalog(module):
    """Finds all imported datasets and collections from a module and submodules.
    """
    # Collect data classes in current module (datasets and collections)
    all_classes = set(
        inspect.getmembers(module, partial(is_member_class, module=module))
    )
    data_classes = {
        x[1] for x in all_classes if is_dataset(x[1]) or is_collection(x[1])
    }

    # Collect data classes from submodules
    submodules = inspect.getmembers(
        module, partial(is_sub_module, module=module)
    )
    for name, submodule in submodules:
        data_classes.update(list_catalog(submodule))

    return data_classes


def describe_catalog(module):
    data_classes = list_catalog(module)
    return {ds.catalog_path(): ds.description() for ds in data_classes}


def camel2snake(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
