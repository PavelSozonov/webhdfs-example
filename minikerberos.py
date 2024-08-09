from minikerberos import Kerberos

# Initialize the Kerberos client
k = Kerberos()

# Get a client credential
rc = k.get_client_credential('HTTP/your.service.com@YOUR.REALM')

# Check if the client credential was obtained successfully
if rc != 0:
    print("Failed to get client credential")
    exit(1)

# Get the delegation token
rc = k.get_server_credential('HTTP/your.service.com@YOUR.REALM')

# Check if the delegation token was obtained successfully
if rc != 0:
    print("Failed to get delegation token")
    exit(1)

# Print the delegation token
print("Delegation token: ", k.get_server_credential_data())