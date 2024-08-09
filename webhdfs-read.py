import requests
from krb5ticket import Krb5Ticket

# Define the principal and keytab file path
principal = "username@REALM"
keytab_file = "/path/to/your.keytab"

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
    print("Delegation token obtained successfully.")
else:
    print(f"Failed to obtain delegation token. Status code: {response.status_code}")
    exit(1)

# Set up the WebHDFS URL for the file you want to access
hdfs_path = '/path/to/hdfs/file.txt'
url = f"http://{host}:{port}/webhdfs/v1{hdfs_path}?op=OPEN&delegation={token}"

# Fetch the file from HDFS using the delegation token
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    with open('local_file.txt', 'wb') as f:
        f.write(response.content)
    print("File downloaded successfully.")
else:
    print(f"Failed to download file. Status code: {response.status_code}")
