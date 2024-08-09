import os
import logging
import requests
from krb5ticket import Krb5Ticket
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the principal and keytab file path
principal = "username@REALM"
keytab_file = "/path/to/your.keytab"

# Optionally set the path to krb5.conf if not in the default location
krb5_conf_path = "/path/to/your/krb5.conf"
os.environ["KRB5_CONFIG"] = krb5_conf_path

# Initialize Kerberos ticket using keytab file
ticket = Krb5Ticket(principal, keytab=keytab_file)
ticket.obtain()

# Set up the URL to obtain the delegation token
host = 'namenode-hostname'
port = 50070  # Default WebHDFS port
url = f"http://{host}:{port}/webhdfs/v1/?op=GETDELEGATIONTOKEN"

# Headers for Kerberos authentication
headers = {
    'Authorization': f'Negotiate {ticket.auth_header()}'
}

# Obtain the delegation token
response = requests.get(url, headers=headers)

if response.status_code == 200:
    token = response.json()['Token']['urlString']
    logging.info("Delegation token obtained successfully.")
else:
    logging.error(f"Failed to obtain delegation token. Status code: {response.status_code}")
    exit(1)

# Function to download a file from HDFS with retries
def download_file_from_hdfs(hdfs_path, local_file_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            url = f"http://{host}:{port}/webhdfs/v1{hdfs_path}?op=OPEN&delegation={token}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"File {hdfs_path} downloaded successfully to {local_file_path}.")
                return
            else:
                logging.error(f"Failed to download file {hdfs_path}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while downloading file {hdfs_path}: {str(e)}")

        # Wait before retrying
        sleep(2 ** attempt)

    logging.error(f"Failed to download file {hdfs_path} after {max_retries} attempts.")

# Path to the HDFS file to download
hdfs_path = '/path/to/hdfs/file.txt'
local_file_path = 'local_file.txt'

# Download the file from HDFS
download_file_from_hdfs(hdfs_path, local_file_path)
