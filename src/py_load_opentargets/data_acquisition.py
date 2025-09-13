import fsspec
import re
import logging
import hashlib
import pyarrow.parquet as pq
import pyarrow as pa
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
        if discovery_uri.startswith(('http://', 'https://')):
            fs, path = fsspec.core.url_to_fs(discovery_uri)
        elif discovery_uri.startswith('gs://'):
            fs, path = fsspec.core.url_to_fs(discovery_uri, token='anon')
        else:
            fs, path = fsspec.core.url_to_fs(discovery_uri, anon=True, timeout=30)
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


def discover_datasets(datasets_uri: str) -> List[str]:
    """
    Discovers available datasets from a top-level directory URI.

    :param datasets_uri: The fsspec-compatible URI to the top-level directory
                         containing all dataset subdirectories (e.g., .../parquet/).
    :return: A sorted list of dataset names (directory names).
    """
    logger.info(f"Discovering datasets at {datasets_uri}...")
    try:
        if datasets_uri.startswith(('http://', 'https://')):
            fs, path = fsspec.core.url_to_fs(datasets_uri)
        elif datasets_uri.startswith('gs://'):
            fs, path = fsspec.core.url_to_fs(datasets_uri, token='anon')
        else:
            fs, path = fsspec.core.url_to_fs(datasets_uri, anon=True)
        all_entries = fs.ls(path, detail=True)

        if not all_entries:
            logger.warning(f"No files or directories found at {datasets_uri}.")
            return []

        # Primary strategy: Use 'type' information if available.
        if 'type' in all_entries[0]:
            dataset_names = [
                Path(entry['name']).name
                for entry in all_entries
                if entry.get('type') == 'directory'
            ]
        # Fallback strategy: Infer directories from paths.
        else:
            logger.info("No 'type' field in listing, inferring directories from path names.")
            all_paths = [entry['name'] for entry in all_entries]
            # A path is considered a dataset if its name is not the same as the parent path's name.
            # This handles cases where the listing includes the parent directory itself.
            dataset_names = sorted(list({Path(p).name for p in all_paths if Path(p).name != Path(path).name}))

        if not dataset_names:
            logger.warning(f"Could not find any datasets (subdirectories) at {datasets_uri}.")
            return []

        logger.info(f"Found {len(dataset_names)} datasets: {sorted(dataset_names)}")
        return sorted(dataset_names)
    except Exception as e:
        logger.error(f"Failed to discover datasets from {datasets_uri}: {e}", exc_info=True)
        return []


def get_checksum_manifest(version: str, checksum_uri_template: str) -> Dict[str, str]:
    """
    Downloads and verifies the checksum manifest for a given Open Targets version.

    :param version: The Open Targets version (e.g., '24.06').
    :param checksum_uri_template: An fsspec-compatible URI template for the release folder.
    :return: A dictionary mapping file paths to their expected SHA1 checksums.
    """
    release_uri = checksum_uri_template.format(version=version)
    # Ensure the URI has a trailing slash for correct path joining
    if not release_uri.endswith('/'):
        release_uri += '/'
    manifest_path = f"{release_uri}release_data_integrity"
    checksum_for_manifest_path = f"{manifest_path}.sha1"
    logger.info(f"Downloading checksum manifest from {release_uri}")

    try:
        if release_uri.startswith(('http://', 'https://')):
            fs, _ = fsspec.core.url_to_fs(release_uri)
        elif release_uri.startswith('gs://'):
            fs, _ = fsspec.core.url_to_fs(release_uri, token='anon')
        else:
            fs, _ = fsspec.core.url_to_fs(release_uri, anon=True, timeout=30)

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


def _verify_remote_file_checksum(remote_url: str, expected_checksum: str):
    """
    Calculates and verifies the SHA1 checksum of a remote file streamed via fsspec.
    """
    logger.debug(f"Verifying checksum for remote file {remote_url}...")
    hasher = hashlib.sha1()
    try:
        with fsspec.open(remote_url, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        actual_checksum = hasher.hexdigest()

        if actual_checksum != expected_checksum:
            raise ValueError(
                f"Checksum mismatch for remote file {remote_url}. "
                f"Expected {expected_checksum}, got {actual_checksum}."
            )
        logger.debug(f"Checksum verified for {remote_url}")
    except Exception as e:
        logger.error(f"Failed to verify checksum for {remote_url}: {e}")
        raise


def _download_and_verify_one_file(
    remote_file: str,
    local_path: Path,
    dataset: str,
    checksum_manifest: Dict[str, str],
    fs,
) -> Path:
    """
    Helper function to download a single file and verify its checksum.
    Designed to be called by `download_dataset`.
    """
    local_file = local_path / Path(remote_file).name
    fs.get(remote_file, str(local_file))
    logger.debug(f"Successfully downloaded {remote_file}")

    # Construct the key for the checksum manifest
    manifest_key = f"output/etl/parquet/{dataset}/{Path(remote_file).name}"
    expected_checksum = checksum_manifest.get(manifest_key)

    if not expected_checksum:
        raise KeyError(f"Checksum not found in manifest for file: {manifest_key}")

    _verify_file_checksum(local_file, expected_checksum)
    return local_file


def download_dataset(
    uri_template: str,
    version: str,
    dataset: str,
    output_dir: Path,
    checksum_manifest: Dict[str, str],
    max_workers: int = 1,
    files_to_download: List[str] = None,
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
    :param files_to_download: An optional list of specific filenames to download.
                              If None, all files in the dataset are downloaded.
    :return: The path to the directory containing the downloaded dataset.
    """
    dataset_url = uri_template.format(version=version, dataset_name=dataset)
    local_path = output_dir / version / dataset
    local_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading dataset '{dataset}' for version '{version}'...")
    logger.info(f"Source URI: {dataset_url}")
    logger.info(f"Local destination: {local_path}")

    try:
        if dataset_url.startswith(('http://', 'https://')):
            fs, path = fsspec.core.url_to_fs(dataset_url)
        elif dataset_url.startswith('gs://'):
            fs, path = fsspec.core.url_to_fs(dataset_url, token='anon')
        else:
            fs, path = fsspec.core.url_to_fs(dataset_url, anon=True, timeout=30)

        manifest_prefix = f"output/etl/parquet/{dataset}/"
        dataset_files = [
            key.replace(manifest_prefix, "")
            for key in checksum_manifest.keys()
            if key.startswith(manifest_prefix)
        ]

        if files_to_download:
            files_to_process = [f for f in dataset_files if f in files_to_download]
        else:
            files_to_process = dataset_files

        if not files_to_process:
            logger.warning(f"No files found for dataset '{dataset}' in the checksum manifest.")
            return local_path

        # Now, construct the full remote paths
        remote_files = [f"{dataset_url}/{fname}" for fname in files_to_process]

        if not remote_files:
            logger.warning("No files matched the download criteria.")
            return local_path

        logger.info(f"Found {len(remote_files)} files to download for dataset '{dataset}'.")

        downloaded_files = []

        if max_workers > 1:
            logger.info(f"Downloading in parallel with {max_workers} workers.")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(
                        _download_and_verify_one_file,
                        remote_file,
                        local_path,
                        dataset,
                        checksum_manifest,
                        fs,
                    ): remote_file
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
                downloaded_files.append(
                    _download_and_verify_one_file(
                        remote_file, local_path, dataset, checksum_manifest, fs
                    )
                )

        logger.info(f"Successfully downloaded and verified dataset '{dataset}' to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download dataset '{dataset}' from {dataset_url}: {e}", exc_info=True)
        raise


def verify_remote_dataset(
    remote_urls: List[str],
    dataset: str,
    checksum_manifest: Dict[str, str],
    max_workers: int = 1,
):
    """
    Verifies the checksums of remote dataset files in parallel.

    :param remote_urls: A list of fsspec-compatible URLs for the dataset files.
    :param dataset: The name of the dataset.
    :param checksum_manifest: A dictionary mapping file paths to their SHA1 checksums.
    :param max_workers: The maximum number of parallel verification threads.
    """
    logger.info(
        f"Verifying checksums for {len(remote_urls)} remote files for dataset '{dataset}'..."
    )

    if max_workers > 1:
        logger.info(f"Verifying in parallel with {max_workers} workers.")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {}
            for url in remote_urls:
                manifest_key = f"output/etl/parquet/{dataset}/{Path(url).name}"
                expected_checksum = checksum_manifest.get(manifest_key)
                if not expected_checksum:
                    raise KeyError(
                        f"Checksum not found in manifest for file: {manifest_key}"
                    )
                future_to_url[
                    executor.submit(_verify_remote_file_checksum, url, expected_checksum)
                ] = url

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()  # Raise exception if verification failed
                except Exception as exc:
                    logger.error(f"Error during verification of {url}: {exc}")
                    raise
    else:
        logger.info("Verifying sequentially.")
        for url in remote_urls:
            manifest_key = f"output/etl/parquet/{dataset}/{Path(url).name}"
            expected_checksum = checksum_manifest.get(manifest_key)
            if not expected_checksum:
                raise KeyError(f"Checksum not found in manifest for file: {manifest_key}")
            _verify_remote_file_checksum(url, expected_checksum)

    logger.info(f"Successfully verified all remote files for dataset '{dataset}'.")


def get_remote_dataset_urls(uri_template: str, version: str, dataset_name: str) -> List[str]:
    """
    Lists all remote Parquet file URLs for a given dataset.

    :param uri_template: The fsspec-compatible URI template for data files.
    :param version: The Open Targets version (e.g., '22.04').
    :param dataset_name: The name of the dataset (e.g., 'targets').
    :return: A sorted list of fsspec-compatible URLs for the Parquet files.
    """
    dataset_url = uri_template.format(version=version, dataset_name=dataset_name)
    logger.info(f"Finding remote file URLs for dataset '{dataset_name}' at: {dataset_url}")
    try:
        if dataset_url.startswith(('http://', 'https://')):
            fs, path = fsspec.core.url_to_fs(dataset_url)
        elif dataset_url.startswith('gs://'):
            fs, path = fsspec.core.url_to_fs(dataset_url, token='anon')
        else:
            fs, path = fsspec.core.url_to_fs(dataset_url, anon=True, timeout=30)
        # This function is now unused, as we get the file list from the manifest.
        # However, we will keep it for now, in case we need it in the future.
        # It will not work for GCS, as it requires listing the bucket.
        if dataset_url.startswith('gs://'):
            logger.warning("Listing remote files is not supported for GCS with anonymous access.")
            return []
        # Use a protocol-aware join for full URLs
        protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]

        if protocol == 'file':
            remote_files = sorted([f"file://{p}" for p in fs.glob(f"{path}/*.parquet")])
        else:
            # The glob returns paths relative to the host, so we need to prepend the protocol and host.
            remote_files = sorted([f"{protocol}://{fs.host}/{p.lstrip('/')}" for p in fs.glob(f"{path}/*.parquet")])

        if not remote_files:
            logger.warning(f"No .parquet files found at {dataset_url}. Check the path and dataset name.")

        logger.info(f"Found {len(remote_files)} remote files for dataset '{dataset_name}'.")
        return remote_files
    except Exception as e:
        logger.error(f"Failed to list remote files for dataset '{dataset_name}': {e}", exc_info=True)
        raise


def get_remote_schema(parquet_urls: List[str]) -> pa.Schema:
    """
    Infers the PyArrow schema from the first Parquet file in a list of remote URLs.

    :param parquet_urls: A list of fsspec-compatible URLs.
    :return: The inferred PyArrow schema.
    """
    if not parquet_urls:
        raise ValueError("Cannot infer schema from an empty list of URLs.")

    first_url = parquet_urls[0]
    logger.info(f"Inferring schema from first remote file: {first_url}")
    try:
        # PyArrow can read the schema directly from a URL using fsspec
        with fsspec.open(first_url, 'rb') as f:
            schema = pq.read_schema(f)
        logger.info("Successfully inferred schema from remote file.")
        return schema
    except Exception as e:
        logger.error(f"Failed to read schema from remote URL '{first_url}': {e}", exc_info=True)
        raise
