import pytest
from src.hdfs_retrieval import read_csv_from_hadoop

def test_read_csv_from_hadoop(mocker):
    mock_fs = mocker.patch("pyhdfs.HdfsClient")
    mock_fs.return_value.exists.return_value = True
    mock_fs.return_value.open.return_value = open('mock_data.csv', 'rb')

    df = read_csv_from_hadoop("localhost", 9870, "/mock_path/mock_data.csv")
    assert not df.empty
