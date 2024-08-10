import kerberos
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED

# Set up the krb5 configuration path if it's not the default
import os
os.environ['KRB5_CONFIG'] = '/path/to/your/krb5.conf'
os.environ['KRB5CCNAME'] = '/tmp/krb5cc'  # Path to credential cache
os.environ['KRB5_KTNAME'] = '/path/to/your/keytab/file.keytab'  # Path to keytab file

# Replace these with appropriate values
principal = 'your-principal@YOUR.REALM'
service = 'HTTP@your-service.com'

def get_kerberos_ticket_with_keytab(principal, service):
    # Initialize Kerberos context with keytab
    try:
        kerberos.authGSSClientInit(service)
        kerberos.authGSSClientStep('')
        kerberos.authGSSClientStep('')  # Empty steps for keytab
        token = kerberos.authGSSClientResponse()
        return token
    except kerberos.KrbError as e:
        print(f"Kerberos error: {e}")
        return None

token = get_kerberos_ticket_with_keytab(principal, service)

if token:
    print("Kerberos token obtained successfully.")
    # Make a request to a Kerberized service
    auth = HTTPKerberosAuth(mutual_authentication=REQUIRED)
    response = requests.get('http://your-kerberized-service.com', auth=auth)
    if response.status_code == 200:
        print("Authenticated request successful.")
    else:
        print(f"Authenticated request failed with status code: {response.status_code}")
else:
    print("Failed to obtain Kerberos token.")
