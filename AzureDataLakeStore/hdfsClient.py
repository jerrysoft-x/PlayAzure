# # create a new client instance
# from pywebhdfs.webhdfs import PyWebHdfsClient
# #
# hdfs = PyWebHdfsClient(host='10.128.122.104', port='50075', user_name='hdfs')
# hdfs.get_file_dir_status('upload_me.txt')

from pyarrow import HdfsClient

# Using libhdfs
hdfs = HdfsClient('10.128.122.104', 50075, 'hdfs')

# hdfs.ls('/')

# # Using libhdfs3
# hdfs_alt = HdfsClient(host, port, username, driver='libhdfs3')

# with hdfs.open('/test/to/file') as f:
#     ...