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

# Function to create a directory in HDFS
def create_hdfs_directory(hdfs_path):
    url = f"http://{host}:{port}/webhdfs/v1{hdfs_path}?op=MKDIRS&delegation={token}"
    response = requests.put(url, headers=headers)
    if response.status_code == 200:
        logging.info(f"Directory {hdfs_path} created successfully.")
    else:
        logging.error(f"Failed to create directory {hdfs_path}. Status code: {response.status_code}")

# Function to upload a file to HDFS with retries
def upload_file_to_hdfs(local_file_path, hdfs_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            url = f"http://{host}:{port}/webhdfs/v1{hdfs_path}?op=CREATE&delegation={token}&overwrite=true"

            # Initiate the file upload
            response = requests.put(url, headers={'Content-Type': 'application/octet-stream'}, allow_redirects=False)

            # Check if the request was redirected
            if response.status_code == 307:
                redirect_url = response.headers['Location']

                # Perform the actual file upload
                with open(local_file_path, 'rb') as f:
                    file_data = f.read()
                    response = requests.put(redirect_url, data=file_data, headers={'Content-Type': 'application/octet-stream'})

                # Check if the upload was successful
                if response.status_code == 201:
                    logging.info(f"File {local_file_path} uploaded successfully.")
                    return
                else:
                    logging.error(f"Failed to upload file {local_file_path}. Status code: {response.status_code}")
            else:
                logging.error(f"Failed to initiate file upload for {local_file_path}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception occurred while uploading file {local_file_path}: {str(e)}")

        # Wait before retrying
        sleep(2 ** attempt)

    logging.error(f"Failed to upload file {local_file_path} after {max_retries} attempts.")

# Path to the local directory to upload
local_directory_path = '/path/to/local/directory'
hdfs_base_path = '/path/to/hdfs/directory'

# Create the base directory in HDFS
create_hdfs_directory(hdfs_base_path)

# Traverse the local directory and upload files
for root, dirs, files in os.walk(local_directory_path):
    for dirname in dirs:
        local_dir_path = os.path.join(root, dirname)
        hdfs_dir_path = os.path.join(hdfs_base_path, os.path.relpath(local_dir_path, local_directory_path)).replace("\\", "/")
        create_hdfs_directory(hdfs_dir_path)
    for filename in files:
        local_file_path = os.path.join(root, filename)
        hdfs_file_path = os.path.join(hdfs_base_path, os.path.relpath(local_file_path, local_directory_path)).replace("\\", "/")
        upload_file_to_hdfs(local_file_path, hdfs_file_path)
