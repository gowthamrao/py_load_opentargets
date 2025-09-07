import fsspec
import re
import logging
from pathlib import Path
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants for data sources
OPENTARGETS_GCS_BASE_URL = "gs://open-targets/platform/"
OPENTARGETS_FTP_HOST = "ftp.ebi.ac.uk"
OPENTARGETS_FTP_PATH = "/pub/databases/opentargets/platform/"


def list_available_versions() -> List[str]:
    """
    Lists available Open Targets release versions from the EBI FTP server.
    FTP is used for listing as GCS bucket listing can require authentication.

    :return: A list of version strings, sorted from newest to oldest.
    """
    logger.info(f"Checking for available versions on FTP host {OPENTARGETS_FTP_HOST}...")
    try:
        # Correctly instantiate the FTP filesystem with the host
        fs = fsspec.filesystem("ftp", host=OPENTARGETS_FTP_HOST, anon=True)

        version_pattern = re.compile(r"^\d{2}\.\d{2}$")
        # List contents of the specific path on the host
        all_paths = fs.ls(OPENTARGETS_FTP_PATH, detail=False)

        # fsspec returns full paths, we just need the directory name
        versions = [
            Path(p).name for p in all_paths if version_pattern.fullmatch(Path(p).name)
        ]

        if not versions:
            logger.warning("Could not find any versions matching the pattern 'YY.MM'.")
            return []

        # Sort versions in descending order (newest first)
        versions.sort(key=lambda s: [int(p) for p in s.split('.')], reverse=True)

        logger.info(f"Found versions: {versions}")
        return versions
    except Exception as e:
        logger.error(f"Failed to list Open Targets versions from FTP: {e}", exc_info=True)
        return []


def download_dataset(version: str, dataset: str, output_dir: Path) -> Path:
    """
    Downloads a specific dataset for a given Open Targets version from GCS.
    GCS is preferred for downloads due to higher speed.

    :param version: The Open Targets version (e.g., '22.04').
    :param dataset: The name of the dataset (e.g., 'targets').
    :param output_dir: The local directory to save the downloaded files.
    :return: The path to the directory containing the downloaded dataset.
    """
    dataset_url = f"{OPENTARGETS_GCS_BASE_URL}{version}/output/etl/parquet/{dataset}/"
    local_path = output_dir / version / dataset

    logger.info(f"Downloading dataset '{dataset}' for version '{version}' from GCS...")
    logger.info(f"Source: {dataset_url}")
    logger.info(f"Destination: {local_path}")

    try:
        fs = fsspec.filesystem("gcs", anon=True)
        local_path.mkdir(parents=True, exist_ok=True)
        fs.get(dataset_url, str(local_path), recursive=True)

        logger.info(f"Successfully downloaded '{dataset}' to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download dataset '{dataset}' from GCS: {e}")
        raise
