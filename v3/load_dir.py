import argparse
import os
import shutil
from pyspark.sql import SparkSession
from org.apache.hadoop.fs import FileSystem, Path
import zipfile

def copy_hdfs_directory_to_local(input_dir, output_zip):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Copy HDFS Directory to Local Archive") \
        .getOrCreate()

    # Get Hadoop configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Create HDFS FileSystem object
    fs = FileSystem.get(hadoop_conf)

    # Define HDFS and local temporary paths
    hdfs_dir_path = Path(input_dir)
    local_temp_dir = "/tmp/hdfs_temp_download"
    local_zip_path = output_zip

    # Create local temporary directory if it doesn't exist
    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)

    # Copy directory from HDFS to local temporary directory
    fs.copyToLocalFile(hdfs_dir_path, Path(local_temp_dir))

    # Compress the local directory into a zip file
    shutil.make_archive(local_zip_path.replace('.zip', ''), 'zip', local_temp_dir)

    # Clean up the local temporary directory
    shutil.rmtree(local_temp_dir)

    # Optionally, stop the Spark session
    spark.stop()

    print(f"Directory copied from {input_dir} and saved as archive {output_zip}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy directory from HDFS and save as local archive.")
    parser.add_argument('input_dir', type=str, help='The HDFS directory path to copy from.')
    parser.add_argument('output_zip', type=str, help='The local archive file path to save to (with .zip extension).')
    args = parser.parse_args()

    copy_hdfs_directory_to_local(args.input_dir, args.output_zip)
