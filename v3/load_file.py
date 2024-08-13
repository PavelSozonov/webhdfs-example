import argparse
from pyspark.sql import SparkSession

# Function to copy HDFS file to local directory
def copy_hdfs_to_local(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Copy HDFS File to Local Directory") \
        .getOrCreate()

    # Get Hadoop configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Create HDFS FileSystem object
    fs = org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    # Define HDFS and local paths
    hdfs_path = org.apache.hadoop.fs.Path(input_path)
    local_path = org.apache.hadoop.fs.Path(output_path)

    # Copy file from HDFS to local file system
    fs.copyToLocalFile(hdfs_path, local_path)

    # Optionally, stop the Spark session
    spark.stop()

    print(f"File copied from {input_path} to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy file from HDFS to local directory.")
    parser.add_argument('input_path', type=str, help='The HDFS file path to copy from.')
    parser.add_argument('output_path', type=str, help='The local file path to copy to.')
    args = parser.parse_args()

    copy_hdfs_to_local(args.input_path, args.output_path)
