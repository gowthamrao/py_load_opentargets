import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"


def get_releases():
    """
    Fetches the list of available data releases from the Open Targets FTP server.

    Returns:
        list: A list of available release versions as strings.
    """
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    releases = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and re.match(r"^\d+\.\d+/$", href):
            releases.append(href.strip('/'))
    return releases


def get_datasets(release):
    """
    Fetches the list of available datasets for a given release.

    Args:
        release (str): The release version (e.g., "25.06").

    Returns:
        list: A list of available dataset names as strings.
    """
    url = f"{BASE_URL}/{release}/output/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    datasets = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and not href.startswith("?") and href != "../":
            datasets.append(href.strip('/'))
    return datasets


def download_dataset(release, dataset, path="."):
    """
    Downloads a dataset from the Open Targets FTP server.

    Args:
        release (str): The release version (e.g., "25.06").
        dataset (str): The name of the dataset to download.
        path (str, optional): The local directory to download the dataset to. Defaults to ".".
    """
    dataset_path = Path(path) / dataset
    dataset_path.mkdir(exist_ok=True)

    url = f"https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/{release}/output/{dataset}"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.endswith(".parquet"):
            file_url = f"{url.rstrip('/')}/{href}"
            file_path = dataset_path / href
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)


def load_dataset(path):
    """
    Loads a downloaded dataset from a local path into a pandas DataFrame.

    Args:
        path (str): The path to the downloaded dataset directory.

    Returns:
        pandas.DataFrame: A DataFrame containing the dataset.
    """
    p = Path(path)
    if not p.is_dir():
        raise ValueError(f"Path {path} is not a directory.")

    parquet_files = list(p.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No .parquet files found in {path}")

    if len(parquet_files) == 1:
        return pd.read_parquet(parquet_files[0])

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True)
