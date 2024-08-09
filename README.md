# WebHDFS File Downloader with Kerberos Authentication

This project provides a script to download files from HDFS using WebHDFS with Kerberos authentication. 
It utilizes a keytab file for authentication and delegation tokens for subsequent requests.

## Requirements

- Python 3.6 or higher
- Kerberos client setup (`kinit`, `krb5.conf` configured)
- Keytab file for authentication