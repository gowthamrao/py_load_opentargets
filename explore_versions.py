import fsspec
from pathlib import Path

# Using a hardcoded recent version to avoid repeated FTP listing
latest_version = "25.06"
print(f"Using version: {latest_version}")

# Path to the checksum files on the FTP server
ftp_base_path = f"ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/{latest_version}/"
checksum_file_path = f"{ftp_base_path}release_data_integrity.sha1"
data_file_path = f"{ftp_base_path}release_data_integrity" # The file being checksummed

print(f"Reading checksum file: {checksum_file_path}")

try:
    # Use fsspec to open and read the files
    fs, _ = fsspec.core.url_to_fs(ftp_base_path, anon=True)

    with fs.open(checksum_file_path, 'r') as f:
        checksum_content = f.read()
        print("\n--- Contents of release_data_integrity.sha1 ---")
        print(checksum_content)
        print("---------------------------------------------")

    with fs.open(data_file_path, 'r') as f:
        data_content = f.read(500) # Read first 500 chars to get a sense of the format
        print(f"\n--- First 500 chars of {Path(data_file_path).name} ---")
        print(data_content)
        print("----------------------------------------------------")

except Exception as e:
    print(f"Error reading files: {e}")
