import pyhdfs
import pandas as pd

def read_csv_from_hadoop(hdfs_host, hdfs_port, file_path):
    """
    Read a CSV file from Hadoop HDFS.
    """
    fs = pyhdfs.HdfsClient(hosts=f"{hdfs_host}:{hdfs_port}")
    if fs.exists(file_path):
        with fs.open(file_path) as f:
            df = pd.read_csv(f)
        return df
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist in HDFS.")
