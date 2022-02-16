import os 
import glob
import pathlib
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from cloudpathlib import CloudPath

from src.config.toml_config import config
# config.toml_config import config

# buils spark session
spark = SparkSession.builder \
    .appName('create_dirs') \
    .getOrCreate()

# instantiate dbutils
dbutils = DBUtils(spark)




def get_existing_dirs():
    """
    Get all existing directories. Create abfss:// formatted URI.
    :param: None
    :returns abfss_paths_list: List of all paths in abfss:// directories.
    :type abfss_paths_list: list
    """

    # root of working blob
    # abfss_root = 'abfss://gdeltdata@gdeltstorage.dfs.core.windows.net'
    abfss_root = f"{config['FS']['PREFIX']}"

    # use cloud path lib for globbing
    az_root = 'az://gdeltdata/'

    # match on all directories
    cloud_glob = CloudPath(az_root).glob('**/*')

    # create list of directories in blob
    dir_terminal_list = []
    for dirs in cloud_glob:



        # modified python 3.9 .removeprefix() method
        def removeprefix(str_: str, prefix: str) -> str:
            if str(str_).startswith(prefix):
                return str(str_)[len(prefix):]
            else:
                return str(str_)[:]

        dir_terminal = removeprefix(str_=dirs, prefix='az://gdeltdata/')

        # dir_terminal = str(dirs).removeprefix('az://gdeltdata/')
        dir_terminal_list.append(dir_terminal)

    # reformat list of directories as abfss:// URIs
    abfss_paths_list = []
    for terminal in dir_terminal_list:

        abfss_path = f'{abfss_root}/{terminal}'
        abfss_paths_list.append(abfss_path)

    return abfss_paths_list


def create_dirs(spark, dirs_list):
    """
    Creates directories for list of desired directories if not already existing.
    :param spark: Spark instance
    :param dirs_list: List of directories to create at at root of ADLS blob.
    :type dirs_list: List of str in format: '/directory'
    """
    existing_dirs = get_existing_dirs()

    for dirs in dirs_list:
        try:
            potential_dir = f"{config['FS']['PREFIX']}{dirs}"

            if potential_dir in existing_dirs:
                print(f"Directory already exists: {config['FS']['PREFIX']}{dirs}")

            else:
                dbutils.fs.mkdirs(f"{config['FS']['PREFIX']}/{dirs}")
                print(f"Successfully created: {config['FS']['PREFIX']}{dirs}")

        except Exception as e:
            print(f'Encountered exception when creating {dirs}\n:{e}')



dirs_list = ['/scripts', '/pex_env', '/gdelt/extract_gkg', '/gdelt/download_metrics',
            '/gdelt/pipeline_metrics_final', '/gdelt/pipeline_metrics_temp',
            '/gdelt/raw_gkg', '/gdelt/transformed_gkg']

create_dirs(spark=spark, dirs_list=dirs_list)



# 10

# In order to access files on a DBFS mount using local file APIs you need to prepend /dbfs to the path, so in your case it should be

# with open('/dbfs/mnt/test_file.json', 'r') as f:
#   for line in f:
#     print(line)
# See more details in the docs at https://docs.databricks.com/data/databricks-file-system.html#local-file-apis especially regarding 
# limitations. With Databricks Runtime 5.5 and below there's a 2GB file limit. With 6.0+ there's no longer such a limit as the FUSE mount has been optimized to deal with larger file sizes.