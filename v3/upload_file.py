import argparse
from pyspark.sql import SparkSession

def upload_file_to_hdfs(local_file, hdfs_dir):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Upload File to HDFS") \
        .getOrCreate()

    # Get Hadoop configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Access Hadoop FileSystem via JVM gateway
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    # Define local file and HDFS path
    local_file_path = spark._jvm.org.apache.hadoop.fs.Path(local_file)
    hdfs_file_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_dir)

    # Copy file from local file system to HDFS
    fs.copyFromLocalFile(local_file_path, hdfs_file_path)

    # Optionally, stop the Spark session
    spark.stop()

    print(f"File {local_file} uploaded to HDFS directory {hdfs_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload file to HDFS.")
    parser.add_argument('local_file', type=str, help='The local file path to upload.')
    parser.add_argument('hdfs_dir', type=str, help='The HDFS directory path to upload to.')
    args = parser.parse_args()

    upload_file_to_hdfs(args.local_file, args.hdfs_dir)
