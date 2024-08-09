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

# Path to the local file to upload
local_file_path = '/path/to/local/file.txt'
hdfs_path = '/path/to/hdfs/file.txt'

# Upload the file to HDFS
upload_file_to_hdfs(local_file_path, hdfs_path)
