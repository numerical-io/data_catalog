from pathlib import Path, PurePath
from datetime import datetime

import pytest
import pandas as pd

import data_catalog.datasets as dd


class TestAbstractDataset:
    def should_infer_its_name(self):
        class MyDataset(dd.AbstractDataset):
            pass

        assert MyDataset.name() == "MyDataset"

    def should_infer_its_path_in_catalog(self):
        class MyDataset(dd.AbstractDataset):
            pass

        assert MyDataset.catalog_path() == "test_datasets.MyDataset"

    def should_infer_description_from_docstring(self):
        class MyDataset(dd.AbstractDataset):
            """This is a docstring."""

            pass

        assert MyDataset.description() == "This is a docstring."

    def should_check_parents_are_datasets(self):
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                parents = ["not_a_dataset"]

                def create(self, a):
                    pass

    def should_have_create_matching_parents(self):
        class ParentDataset(dd.AbstractDataset):
            pass

        # Case: create has too few arguments
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                parents = [ParentDataset]

                def create(self):
                    pass

        # Case: create has too many arguments
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                parents = [ParentDataset]

                def create(self, a, b):
                    pass

        # Case: create should have no arguments
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                parents = []

                def create(self, a):
                    pass

        # Case: create should have no arguments, because parents is absent
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                def create(self, a):
                    pass

        # Case: create should exist, because dataset has parents
        with pytest.raises(ValueError):

            class ChildDataset(dd.AbstractDataset):
                parents = [ParentDataset]

        # All these should not raise exceptions
        class ChildDataset(dd.AbstractDataset):
            parents = []

        class ChildDataset(dd.AbstractDataset):
            def create(self):
                pass

        class ChildDataset(dd.AbstractDataset):
            parents = [ParentDataset]

            def create(self, df):
                pass

    def should_allow_no_parents(self):
        # When no parents are given, the parents are set to an empty list
        class ParentDataset(dd.AbstractDataset):
            pass

        assert isinstance(ParentDataset.parents, list)
        assert len(ParentDataset.parents) == 0

    def should_have_same_class_and_object_hash(self):
        class MyDataset(dd.AbstractDataset):
            pass

        assert hash(MyDataset) == hash("test_datasets.MyDataset")
        assert hash(MyDataset({})) == hash("test_datasets.MyDataset")

    def should_be_singleton_class(self):
        class MyDataset(dd.AbstractDataset):
            pass

        class MyOtherDataset(dd.AbstractDataset):
            pass

        # All instances are identical
        assert MyDataset({}) == MyDataset({})

        # Instance and class are identical (!)
        assert MyDataset({}) == MyDataset

        # Different classes / instance are not equal
        assert MyDataset != MyOtherDataset
        assert MyDataset != MyOtherDataset({})

    def should_save_context(self):
        context = {"a": 1, "b": 2}
        a = dd.AbstractDataset(context)
        assert a.context == context


class TestFileDataset:
    def should_infer_missing_relative_path(self, tmpdir):
        class MyDataset(dd.FileDataset):
            def create(self):
                pass

        relative_path = MyDataset.relative_path.as_posix()
        assert relative_path == "MyDataset.dat"

    def should_ensure_relative_path_is_path_object(self):
        class MyDataset(dd.FileDataset):
            relative_path = "test_datasets/MyDataset.dat"

        assert isinstance(MyDataset.relative_path, PurePath)

    def should_tell_full_path(self, tmpdir):
        class MyDataset(dd.FileDataset):
            def create(self):
                pass

        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        expected_path = Path(tmpdir) / "MyDataset.dat"
        assert MyDataset(context).path() == expected_path

    def should_tell_last_update_time(self):
        datasets_path = Path(__file__).parent / "examples" / "datasets"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        class MyDataset(dd.FileDataset):
            relative_path = "raw_dataset.csv"

        a = MyDataset(context)
        last_update_time = a.last_update_time()

        assert (
            last_update_time > datetime.fromtimestamp(0).astimezone()
        ) and isinstance(last_update_time, datetime)

        class MyOtherDataset(dd.FileDataset):
            relative_path = "non_existant_dataset.dat"

        a = MyOtherDataset(context)
        assert a.last_update_time() == datetime.fromtimestamp(0).astimezone()

    def should_tell_if_exists(self):
        datasets_path = Path(__file__).parent / "examples" / "datasets"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        class MyDataset(dd.FileDataset):
            relative_path = "raw_dataset.csv"

        a = MyDataset(context)
        assert a.exists()

        class MyOtherDataset(dd.FileDataset):
            relative_path = "non_existant_dataset.dat"

        a = MyOtherDataset(context)
        assert not a.exists()

    def should_let_override_file_extension(self, tmpdir):
        class MyDataset(dd.FileDataset):
            file_extension = "data"

            def create(self):
                pass

        relative_path = MyDataset.relative_path.as_posix()
        assert relative_path == "MyDataset.data"

    def should_fail_to_write(self, tmpdir):
        class MyDataset(dd.FileDataset):
            def create(self):
                pass

        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = MyDataset(context)
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(NotImplementedError):
            a.write(df)

    def should_fail_to_read(self, tmpdir):
        datasets_path = Path(__file__).parent / "examples" / "datasets"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        class MyDataset(dd.FileDataset):
            relative_path = "raw_dataset.csv"

        a = MyDataset(context)
        with pytest.raises(NotImplementedError):
            df = a.read()

    def should_save_context(self, tmpdir):
        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = dd.FileDataset(context)
        assert a.context == context


class TestCsvDataset:
    def should_write(self, tmpdir):
        class RawDataset(dd.CsvDataset):
            relative_path = "raw/dataset.csv"

        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = RawDataset(context)
        print(a.write_mode())
        df = pd.DataFrame({"a": [1, 2]})
        a.write(df)

        # Check what has been written
        imported_df = pd.read_csv(a.path(), index_col=0)
        assert df.equals(imported_df)

    def should_write_with_kwargs(self, tmpdir):
        class RawDataset(dd.CsvDataset):
            relative_path = "raw/dataset.csv"
            write_kwargs = {"index": False}

        context = {"catalog_uri": Path(tmpdir).absolute().as_uri()}
        a = RawDataset(context)
        print(a.write_mode())
        df = pd.DataFrame({"a": [1, 2]})
        a.write(df)

        # Check what has been written
        imported_df = pd.read_csv(a.path())
        assert df.equals(imported_df)

    def should_read(self):
        datasets_path = Path(__file__).parent / "examples" / "datasets"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        class RawDataset(dd.CsvDataset):
            relative_path = "raw_dataset.csv"

        a = RawDataset(context)
        df = a.read()
        assert "a" in df.columns
        assert df.shape == (2, 3)

    def should_read_with_kwargs(self):
        datasets_path = Path(__file__).parent / "examples" / "datasets"
        context = {"catalog_uri": datasets_path.absolute().as_uri()}

        class RawDataset(dd.CsvDataset):
            relative_path = "raw_dataset.csv"
            read_kwargs = {"index_col": "a"}

        a = RawDataset(context)
        df = a.read()

        assert "a" not in df.columns
        assert df.index.name == "a"
        assert df.shape == (2, 2)
