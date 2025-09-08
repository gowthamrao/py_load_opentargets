import fsspec
import re
import logging
from pathlib import Path
from typing import List

# Configure logging
logger = logging.getLogger(__name__)

def list_available_versions(discovery_uri: str) -> List[str]:
    """
    Lists available Open Targets release versions from any fsspec-compatible URI.

    :param discovery_uri: The fsspec-compatible URI to list versions from.
    :return: A list of version strings, sorted from newest to oldest.
    """
    logger.info(f"Checking for available versions at {discovery_uri}...")
    try:
        # Use fsspec to open the URI, anon=True is a good default for public data
        fs, path = fsspec.core.url_to_fs(discovery_uri, anon=True)

        version_pattern = re.compile(r"^\d{2}\.\d{2}$")
        all_paths = fs.ls(path, detail=False)

        # fsspec returns full paths, we just need the directory name
        versions = [
            Path(p).name for p in all_paths if fs.isdir(p) and version_pattern.fullmatch(Path(p).name)
        ]

        if not versions:
            logger.warning(f"Could not find any versions matching the pattern 'YY.MM' at the specified URI.")
            return []

        # Sort versions in descending order (newest first)
        versions.sort(key=lambda s: [int(p) for p in s.split('.')], reverse=True)

        logger.info(f"Found versions: {versions}")
        return versions
    except Exception as e:
        logger.error(f"Failed to list Open Targets versions from {discovery_uri}: {e}", exc_info=True)
        return []


from concurrent.futures import ThreadPoolExecutor, as_completed

def download_dataset(
    uri_template: str, version: str, dataset: str, output_dir: Path, max_workers: int = 1
) -> Path:
    """
    Downloads a specific dataset for a given Open Targets version from a templated URI.
    If the remote path is a directory, it downloads all files in parallel.

    Note:
        This function does not perform checksum validation (FRD R.3.1.5) because,
        after investigation, the Open Targets FTP and GCS sources do not appear
        to provide manifest files or checksums for their data releases.

    :param uri_template: The fsspec-compatible URI template.
    :param version: The Open Targets version (e.g., '22.04').
    :param dataset: The name of the dataset (e.g., 'targets').
    :param output_dir: The local directory to save the downloaded files.
    :param max_workers: The maximum number of parallel download threads.
    :return: The path to the directory containing the downloaded dataset.
    """
    dataset_url = uri_template.format(version=version, dataset_name=dataset)
    local_path = output_dir / version / dataset
    local_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading dataset '{dataset}' for version '{version}'...")
    logger.info(f"Source URI: {dataset_url}")
    logger.info(f"Local destination: {local_path}")

    try:
        fs, path = fsspec.core.url_to_fs(dataset_url, anon=True)
        all_files = fs.glob(f"{path}/*.parquet")

        if not all_files:
            logger.warning(f"No .parquet files found at {dataset_url}. Check the path and dataset name.")
            return local_path

        logger.info(f"Found {len(all_files)} files to download for dataset '{dataset}'.")

        if max_workers > 1:
            logger.info(f"Downloading in parallel with {max_workers} workers.")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(fs.get, remote_file, str(local_path / Path(remote_file).name)): remote_file
                    for remote_file in all_files
                }
                for future in as_completed(future_to_file):
                    remote_file = future_to_file[future]
                    try:
                        future.result()
                        logger.debug(f"Successfully downloaded {remote_file}")
                    except Exception as exc:
                        logger.error(f"Error downloading {remote_file}: {exc}")
                        # Depending on desired behavior, you might want to raise here
                        # or collect errors and raise a summary exception at the end.
                        raise
        else:
            logger.info("Downloading sequentially.")
            fs.get(path, str(local_path), recursive=True)

        logger.info(f"Successfully downloaded dataset '{dataset}' to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download dataset '{dataset}' from {dataset_url}: {e}", exc_info=True)
        raise
