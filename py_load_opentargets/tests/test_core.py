import pytest
from unittest.mock import MagicMock, call, mock_open
import pandas as pd
from pathlib import Path
import requests

from py_load_opentargets import core


@pytest.fixture
def mock_requests_get(mocker):
    return mocker.patch("requests.get")


def test_get_releases(mock_requests_get):
    mock_response = MagicMock()
    mock_response.text = """
    <html><body>
        <a href="1.0/">1.0/</a>
        <a href="2.0/">2.0/</a>
        <a href="latest/">latest/</a>
    </body></html>
    """
    mock_requests_get.return_value = mock_response

    releases = core.get_releases()

    assert releases == ["1.0", "2.0"]
    mock_requests_get.assert_called_once_with(core.BASE_URL)


def test_get_datasets(mock_requests_get):
    mock_response = MagicMock()
    mock_response.text = """
    <html><body>
        <a href="../">../</a>
        <a href="?C=N;O=D">?C=N;O=D</a>
        <a href="dataset1/">dataset1/</a>
        <a href="dataset2/">dataset2/</a>
    </body></html>
    """
    mock_requests_get.return_value = mock_response

    datasets = core.get_datasets("25.06")

    assert datasets == ["dataset1", "dataset2"]
    url = f"{core.BASE_URL}/25.06/output/"
    mock_requests_get.assert_called_once_with(url)


def test_download_dataset(mock_requests_get, mocker):
    # Mock the directory listing response
    list_response = MagicMock(spec=requests.Response)
    list_response.text = '<html><body><a href="file1.parquet">file1.parquet</a></body></html>'
    list_response.raise_for_status = MagicMock()

    # Mock the file download response
    file_response = MagicMock(spec=requests.Response)
    file_response.raise_for_status = MagicMock()
    file_response.__enter__.return_value = file_response
    file_response.iter_content.return_value = iter([b"parquet_data"])

    mock_requests_get.side_effect = [list_response, file_response]

    mocker.patch("pathlib.Path.mkdir")
    m = mock_open()
    mocker.patch("builtins.open", m)

    core.download_dataset("25.06", "dataset1", "/tmp/path")

    # Check calls to requests.get
    expected_list_url = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/25.06/output/dataset1"
    expected_file_url = f"{expected_list_url}/file1.parquet"
    mock_requests_get.assert_has_calls([
        call(expected_list_url),
        call(expected_file_url, stream=True)
    ])

    # Check that the file was written
    m.assert_called_once_with(Path("/tmp/path/dataset1/file1.parquet"), "wb")
    handle = m()
    handle.write.assert_called_once_with(b"parquet_data")


def test_load_dataset_single_file(mocker):
    # Test loading a single parquet file
    mocker.patch("pathlib.Path.is_dir", return_value=True)
    mocker.patch("pathlib.Path.glob", return_value=[Path("dummy.parquet")])
    mock_read_parquet = mocker.patch("pandas.read_parquet")

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    mock_read_parquet.return_value = df

    loaded_df = core.load_dataset("dummy_path")

    pd.testing.assert_frame_equal(loaded_df, df)
    mock_read_parquet.assert_called_once_with(Path("dummy.parquet"))


def test_load_dataset_multiple_files(mocker):
    # Test loading and concatenating multiple parquet files
    mocker.patch("pathlib.Path.is_dir", return_value=True)
    mocker.patch("pathlib.Path.glob", return_value=[Path("file1.parquet"), Path("file2.parquet")])

    df1 = pd.DataFrame({"a": [1], "b": [2]})
    df2 = pd.DataFrame({"a": [3], "b": [4]})
    expected_df = pd.DataFrame({"a": [1, 3], "b": [2, 4]}, index=[0, 1])

    mock_read_parquet = mocker.patch("pandas.read_parquet", side_effect=[df1, df2])
    mock_concat = mocker.patch("pandas.concat", return_value=expected_df)

    loaded_df = core.load_dataset("dummy_path")

    pd.testing.assert_frame_equal(loaded_df, expected_df)
    mock_read_parquet.assert_has_calls([
        call(Path("file1.parquet")),
        call(Path("file2.parquet")),
    ])
    mock_concat.assert_called_once()


def test_load_dataset_no_dir(mocker):
    mocker.patch("pathlib.Path.is_dir", return_value=False)
    with pytest.raises(ValueError):
        core.load_dataset("non_existent_path")


def test_load_dataset_no_parquet(mocker):
    mocker.patch("pathlib.Path.is_dir", return_value=True)
    mocker.patch("pathlib.Path.glob", return_value=[])
    with pytest.raises(ValueError):
        core.load_dataset("empty_dir")
