import pytest
from pathlib import Path

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


# class TestFolderIterator:
#     def should_list_filenames_without_extension(self):
#         file_system = df.LocalFileSystem(
#             Path(__file__).parent / "examples" / "datasets"
#         )
#         iterator = du.FolderIterator("dir_to_list")
#         assert iterator(file_system) == {"file_a", "file_b"}


class TestKeysFromFolder:
    def should_list_filenames_without_extension(self):
        class ClassWithFileSystem:
            file_system = df.LocalFileSystem(
                Path(__file__).parent / "examples" / "datasets"
            )
        iterator = du.keys_from_folder("dir_to_list")
        assert iterator(ClassWithFileSystem()) == {"file_a", "file_b"}
