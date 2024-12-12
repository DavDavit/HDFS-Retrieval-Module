import os
import pyhdfs

def retrieve_fits_files(hdfs_host, hdfs_port, folder_path, ids, local_save_dir):
    """
    Retrieve FITS files from HDFS to the local directory.
    """
    fs = pyhdfs.HdfsClient(hosts=f"{hdfs_host}:{hdfs_port}")
    os.makedirs(local_save_dir, exist_ok=True)

    for file_id in ids:
        remote_path = os.path.join(folder_path, f"{file_id}.fits")
        local_path = os.path.join(local_save_dir, f"{file_id}.fits")

        if fs.exists(remote_path):
            with fs.open(remote_path, 'rb') as src, open(local_path, 'wb') as dest:
                dest.write(src.read())
            print(f"Retrieved: {remote_path}")
        else:
            print(f"File not found: {remote_path}")
