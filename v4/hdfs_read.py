from pyspark.sql import SparkSession
import os

# Set the Python interpreter for PySpark
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

# Initialize Spark session in cluster mode
spark = (
    SparkSession.builder
    .master("yarn")
    .appName("ReadHDFSFileClusterMode")
    .config("spark.submit.deployMode", "cluster")
    .config("spark.kerberos.keytab", "/home/jovyan/SozonovPS-08.keytab")
    .config("spark.kerberos.principal", "sozonovps@ISB")
    .config("spark.pyspark.python", "/usr/bin/python3")
    .getOrCreate()
)

# Example of reading a file from HDFS
hdfs_file_path = "hdfs://namenode:9000/path/to/your/file"

# Read the file into a DataFrame
df = spark.read.text(hdfs_file_path)

# Show the content of the DataFrame
df.show()

# Stop the Spark session
spark.stop()
