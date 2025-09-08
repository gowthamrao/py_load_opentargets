import fsspec
import re
import logging
import hashlib
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        fs, path = fsspec.core.url_to_fs(discovery_uri, anon=True)
        version_pattern = re.compile(r"^\d{2}\.\d{2}$")
        all_paths = fs.ls(path, detail=False)

        # For FTP, fsspec returns relative paths, so isdir check might fail.
        # The pattern match is reliable enough for the Open Targets source.
        versions = [
            Path(p).name for p in all_paths if version_pattern.fullmatch(Path(p).name)
        ]

        if not versions:
            logger.warning(f"Could not find any versions matching pattern 'YY.MM' at {discovery_uri}.")
            return []

        versions.sort(key=lambda s: [int(p) for p in s.split('.')], reverse=True)
        logger.info(f"Found versions: {versions}")
        return versions
    except Exception as e:
        logger.error(f"Failed to list Open Targets versions from {discovery_uri}: {e}", exc_info=True)
        return []


def get_checksum_manifest(version: str, checksum_uri_template: str) -> Dict[str, str]:
    """
    Downloads and verifies the checksum manifest for a given Open Targets version.

    :param version: The Open Targets version (e.g., '24.06').
    :param checksum_uri_template: An fsspec-compatible URI template for the release folder.
    :return: A dictionary mapping file paths to their expected SHA1 checksums.
    """
    release_uri = checksum_uri_template.format(version=version)
    manifest_path = f"{release_uri}release_data_integrity"
    checksum_for_manifest_path = f"{manifest_path}.sha1"
    logger.info(f"Downloading checksum manifest from {release_uri}")

    try:
        fs, _ = fsspec.core.url_to_fs(release_uri, anon=True)

        # 1. Download the manifest file and its checksum file
        with fs.open(checksum_for_manifest_path, 'r') as f:
            expected_checksum = f.read().split()[0]
        with fs.open(manifest_path, 'rb') as f:
            manifest_content = f.read()

        # 2. Verify the manifest file itself
        actual_checksum = hashlib.sha1(manifest_content).hexdigest()
        if actual_checksum != expected_checksum:
            raise ValueError(
                f"Checksum mismatch for manifest file {manifest_path}. "
                f"Expected {expected_checksum}, got {actual_checksum}."
            )
        logger.info("Checksum manifest file verified successfully.")

        # 3. Parse the manifest into a dictionary
        manifest_text = manifest_content.decode('utf-8')
        checksum_map = {}
        for line in manifest_text.strip().split('\n'):
            checksum, file_path = line.split(maxsplit=1)
            # Normalize path to be relative, e.g., 'output/...'
            normalized_path = file_path.lstrip('./')
            checksum_map[normalized_path] = checksum

        logger.info(f"Loaded {len(checksum_map)} checksums from manifest.")
        return checksum_map
    except Exception as e:
        logger.error(f"Failed to get or verify checksum manifest from {release_uri}: {e}", exc_info=True)
        raise


def _verify_file_checksum(file_path: Path, expected_checksum: str):
    """Calculates and verifies the SHA1 checksum of a local file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found for checksum verification: {file_path}")

    hasher = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    actual_checksum = hasher.hexdigest()

    if actual_checksum != expected_checksum:
        raise ValueError(
            f"Checksum mismatch for {file_path}. "
            f"Expected {expected_checksum}, got {actual_checksum}."
        )
    logger.debug(f"Checksum verified for {file_path}")


def download_dataset(
    uri_template: str,
    version: str,
    dataset: str,
    output_dir: Path,
    checksum_manifest: Dict[str, str],
    max_workers: int = 1,
) -> Path:
    """
    Downloads a specific dataset for a given Open Targets version from a templated URI,
    verifying the checksum of each downloaded file.

    :param uri_template: The fsspec-compatible URI template for data files.
    :param version: The Open Targets version (e.g., '22.04').
    :param dataset: The name of the dataset (e.g., 'targets').
    :param output_dir: The local directory to save the downloaded files.
    :param checksum_manifest: A dictionary mapping file paths to their SHA1 checksums.
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
        remote_files = fs.glob(f"{path}/*.parquet")

        if not remote_files:
            logger.warning(f"No .parquet files found at {dataset_url}. Check the path and dataset name.")
            return local_path

        logger.info(f"Found {len(remote_files)} files to download for dataset '{dataset}'.")

        downloaded_files = []

        def download_and_verify(remote_file):
            local_file = local_path / Path(remote_file).name
            fs.get(remote_file, str(local_file))
            logger.debug(f"Successfully downloaded {remote_file}")

            # Only perform checksum validation if a manifest is provided
            if checksum_manifest:
                # The manifest paths are relative to the version root, e.g.,
                # 'output/etl/parquet/targets/part-000.parquet'
                manifest_key = f"output/etl/parquet/{dataset}/{Path(remote_file).name}"
                expected_checksum = checksum_manifest.get(manifest_key)

                if expected_checksum:
                    _verify_file_checksum(local_file, expected_checksum)
                else:
                    # If a manifest is provided, all files must be in it
                    raise KeyError(f"Checksum not found in manifest for file: {manifest_key}")
            return local_file

        if max_workers > 1:
            logger.info(f"Downloading in parallel with {max_workers} workers.")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(download_and_verify, remote_file): remote_file
                    for remote_file in remote_files
                }
                for future in as_completed(future_to_file):
                    remote_file = future_to_file[future]
                    try:
                        downloaded_files.append(future.result())
                    except Exception as exc:
                        logger.error(f"Error during download or verification of {remote_file}: {exc}")
                        raise
        else:
            logger.info("Downloading sequentially.")
            for remote_file in remote_files:
                downloaded_files.append(download_and_verify(remote_file))

        logger.info(f"Successfully downloaded and verified dataset '{dataset}' to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download dataset '{dataset}' from {dataset_url}: {e}", exc_info=True)
        raise
