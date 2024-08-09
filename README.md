# WebHDFS File Uploader/Downloader with Kerberos Authentication

This project provides scripts to upload and download files from HDFS using WebHDFS with Kerberos authentication. It utilizes a keytab file for authentication and delegation tokens for subsequent requests.

## Requirements

- Python 3.9 or higher
- Kerberos client setup (`krb5.conf` configured)
- Keytab file for authentication

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/webhdfs-kerberos.git
   cd webhdfs-kerberos
   ```

2. Install the required Python packages:
   ```sh
   pip install -r requirements.txt
   ```

## Configuration

1. **Kerberos Setup:**
   Ensure your Kerberos client is configured properly. Set the `KRB5_CONFIG` environment variable to point to your `krb5.conf` file if it's not in the default location.

2. **Edit the Scripts:**
   Update the `principal`, `keytab_file`, `host`, `port`, and paths (both local and HDFS) in the `webhdfs_upload_file.py`, `webhdfs_upload_directory.py`, and `webhdfs_download_file.py` scripts.

## Usage

### Uploading a Single File

Run the script to upload a single file to HDFS:
```sh
python webhdfs_upload_file.py
```

### Uploading a Directory

Run the script to upload a directory to HDFS:
```sh
python webhdfs_upload_directory.py
```

### Downloading a File

Run the script to download a file from HDFS:
```sh
python webhdfs_download_file.py
```

The files will be uploaded or downloaded according to the paths specified in the scripts.

## Logging

The scripts use Python's logging module to log info and error messages. The logs provide detailed information about the upload and download processes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
