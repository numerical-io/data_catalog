from pathlib import Path, PurePosixPath
from datetime import datetime

import pytest

from data_catalog.file_systems import (
    LocalFileSystem,
    S3FileSystem,
    create_filesystem_from_uri,
)


@pytest.fixture
def local_file_system():
    return LocalFileSystem(Path(__file__).parent / "examples" / "datasets")


class TestLocalFileSystem:
    def should_tell_full_path(self, local_file_system):
        path = local_file_system.full_path("raw_dataset.csv")
        assert path == local_file_system.root / "raw_dataset.csv"

    def should_tell_uri(self, local_file_system):
        uri = local_file_system.uri("raw_dataset.csv")
        assert uri.startswith("file://")

    def should_detect_file_existence(self, local_file_system):
        assert local_file_system.exists("raw_dataset.csv")
        assert not local_file_system.exists("not_existing_dataset.csv")

    def should_get_file_update_time(self, local_file_system):
        last_update_time = local_file_system.last_update_time("raw_dataset.csv")
        assert isinstance(last_update_time, datetime)

    def should_open_files(self, local_file_system):
        with local_file_system.open("raw_dataset.csv") as file:
            contents = file.read()
        assert contents[0] == "a"

    def should_make_directories(self, tmpdir):
        fs = LocalFileSystem(tmpdir)
        fs.mkdir("mytest")
        assert fs.exists("mytest")

    def should_create_intermediate_dir_when_writing_files(self, tmpdir):
        fs = LocalFileSystem(tmpdir)
        with fs.open("mytest_dir/other_dir/mytest_file.txt", "w") as file:
            file.write("aaa")
        assert fs.exists("mytest_dir")
        assert fs.exists("mytest_dir/other_dir")
        assert fs.exists("mytest_dir/other_dir/mytest_file.txt")

    def should_list_files(self, local_file_system):
        filenames = local_file_system.listdir("dir_to_list")
        assert set(filenames) == {"file_a.dat", "file_b.dat"}

        filenames = local_file_system.listdir(
            "dir_to_list", with_hidden_files=True
        )
        expected_filenames = {"file_a.dat", "file_b.dat", ".hidden_file.dat"}
        assert set(filenames) == expected_filenames


@pytest.fixture
def s3_file_system():
    return S3FileSystem("my-bucket/data/catalog")


class TestS3FileSystem:
    def should_tell_full_path(self, s3_file_system):
        path = s3_file_system.full_path("raw_dataset.csv")
        assert path == "my-bucket/data/catalog/raw_dataset.csv"

    def should_tell_uri(self, s3_file_system):
        uri = s3_file_system.uri("raw_dataset.csv")
        assert uri == "s3://my-bucket/data/catalog/raw_dataset.csv"


class TestCreateFilesystemFromUri:
    def should_infer_correct_filesystem(self):
        file_uri = PurePosixPath("/tmp/some/path").as_uri()
        fs_a = create_filesystem_from_uri(file_uri)
        assert isinstance(fs_a, LocalFileSystem)

        fs_b = create_filesystem_from_uri("s3://some/s3/path")
        assert isinstance(fs_b, S3FileSystem)
        assert str(fs_b.root) == "some/s3/path"
