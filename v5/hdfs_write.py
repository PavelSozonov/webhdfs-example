import kfp
from kfp.v2.dsl import component, Input, Output, Dataset
from kfp.v2 import dsl
import requests

@component(
    packages_to_install=['requests'],
    base_image='python:3.12'
)
def download_file_from_hdfs(delegation_token: str, hdfs_file_path: str, local_file_path: Output[Dataset]):
    """
    Downloads a file from HDFS using WebHDFS.

    Args:
        delegation_token (str): Delegation token for accessing HDFS.
        hdfs_file_path (str): Path to the file in HDFS.
        local_file_path (Output[Dataset]): Local path where the file should be saved.
    """
    # Construct WebHDFS URL
    webhdfs_url = f"http://<your-namenode-host>:<port>/webhdfs/v1{hdfs_file_path}?op=OPEN&delegation={delegation_token}"

    # Send the GET request to WebHDFS
    response = requests.get(webhdfs_url, allow_redirects=True)

    # Check if the request was successful
    if response.status_code == 200:
        # Write the content to the local file
        with open(local_file_path.path, 'wb') as f:
            f.write(response.content)
    else:
        raise Exception(f"Failed to download file from HDFS. Status code: {response.status_code}, Response: {response.text}")

@component(
    packages_to_install=['requests'],
    base_image='python:3.12'
)
def upload_file_to_hdfs(delegation_token: str, local_file_path: Input[Dataset], hdfs_file_path: str):
    """
    Uploads a file to HDFS using WebHDFS.

    Args:
        delegation_token (str): Delegation token for accessing HDFS.
        local_file_path (Input[Dataset]): Local path of the file to be uploaded.
        hdfs_file_path (str): Path to the file in HDFS.
    """
    # Construct WebHDFS URL
    webhdfs_url = f"http://<your-namenode-host>:<port>/webhdfs/v1{hdfs_file_path}?op=CREATE&delegation={delegation_token}"

    # Open the local file and read its contents
    with open(local_file_path.path, 'rb') as f:
        file_data = f.read()

    # Send the PUT request to WebHDFS
    response = requests.put(webhdfs_url, data=file_data)

    # Check if the request was successful
    if response.status_code != 201:
        raise Exception(f"Failed to upload file to HDFS. Status code: {response.status_code}, Response: {response.text}")

if __name__ == "__main__":
    # Define the pipeline
    @dsl.pipeline(
        name='HDFS File Transfer Pipeline',
        description='A pipeline to download and upload files to HDFS using WebHDFS'
    )
    def hdfs_transfer_pipeline(delegation_token: str, hdfs_download_file_path: str, hdfs_upload_file_path: str):
        # Download file from HDFS
        download_file_task = download_file_from_hdfs(
            delegation_token=delegation_token,
            hdfs_file_path=hdfs_download_file_path,
            local_file_path=dsl.Output(dsl.Dataset)
        )

        # Upload file to HDFS
        upload_file_task = upload_file_to_hdfs(
            delegation_token=delegation_token,
            local_file_path=download_file_task.outputs['local_file_path'],
            hdfs_file_path=hdfs_upload_file_path
        )

    # Compile the pipeline
    kfp.v2.compiler.Compiler().compile(hdfs_transfer_pipeline, 'hdfs_transfer_pipeline.yaml')
