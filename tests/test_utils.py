import sys
import pytest
from pathlib import Path

import data_catalog
import data_catalog.datasets as ds
import data_catalog.utils as du
import data_catalog.file_systems as df


class TestFindMandatoryArguments:
    def should_detect_only_mandatory_arguments(self):
        def my_func(a, b, c=3):
            pass

        arguments = du._find_mandatory_arguments(my_func)
        assert arguments == ["a", "b"]

    def should_include_mandatory_kwargs(self):
        def my_func(a, b, *, c, d=7):
            pass

        arguments = du._find_mandatory_arguments(my_func)
        assert arguments == ["a", "b", "c"]


class TestKeysFromFolder:
    def should_list_filenames_without_extension(self):
        class ClassWithFileSystem:
            file_system = df.LocalFileSystem(
                Path(__file__).parent / "examples" / "datasets"
            )

        iterator = du.keys_from_folder("dir_to_list")
        assert iterator(ClassWithFileSystem()) == {"file_a", "file_b"}


class DummyDataset(ds.FileDataset):
    """A Dataset for testing catalog lists."""

    pass


def dummy_function():
    pass


class TestIsSubModule:
    def should_recognize_submodules_only(self):
        assert du.is_sub_module(du, data_catalog)
        assert not du.is_sub_module(data_catalog, du)
        assert not du.is_sub_module(du, du)
        assert not du.is_sub_module(du, pytest)


class TestIsMemberClass:
    def should_recognize_classes_defined_in_module(self):
        module = sys.modules[__name__]
        assert du.is_member_class(DummyDataset, module)
        assert not du.is_member_class(dummy_function, module)
        assert not du.is_member_class(ds.AbstractDataset, module)


class TestDescribeCatalog:
    def should_describe_classes(self):
        module = sys.modules[__name__]
        descriptions = du.describe_catalog(module)
        print(descriptions)
        assert DummyDataset.catalog_path() in descriptions
        assert (
            descriptions[DummyDataset.catalog_path()]
            == DummyDataset.description()
        )
