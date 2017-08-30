from azure.datalake.store import lib, core
from azure.datalake.store.multithread import ADLUploader, ADLDownloader
import re

tenant_id = 'adf10e2b-b6e9-41d6-be2f-c12bb566019c'
# store_name = 'adljjlitest'
# client_id = 'e649e5f4-0791-4447-9022-a4d3c6725f90'
# client_secret = 'qE3Nb2Zvm4TLBIjI3+ueurEnFeTmBI13BGv/ItBZz/E='

# store_name = 'oceanspacedlsdev'
# client_id = '66dc26f5-4f15-43d3-92e8-bbeb1e3f4ff8'
# client_secret = '09oxNCRFr07TbUETe8n7xefVv/FBxwsWmhC8gDOVHUQ='

store_name = 'etaservicedlsdev'
client_id = '960726f7-f791-4afc-8b92-35d8a2184534'
client_secret = '0uRgQ4SGiB6PaLmTR41sR43NSiIDZqGoIuEpeUm9/8Y='

token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
adl = core.AzureDLFileSystem(store_name=store_name, token=token)

# adl.ls('')
# adl.mkdir('/mytestfolder')
print(adl.ls('archive'))  # list files in the root directory
# ['mynewfolder/dwarchitecture.png', 'mynewfolder/oop_subclass.py', 'mynewfolder/poem.txt']

# ADLUploader(adl, lpath='If_You_Forget_Me.txt', rpath='/mytestfolder/If_You_Forget_Me.txt', nthreads=64,
#             overwrite=True, buffersize=4194304, blocksize=4194304)

# ADLDownloader(adl, lpath='', rpath='/mynewfolder',
#               nthreads=64, overwrite=True, buffersize=4194304, blocksize=4194304)
# foldertodelete

# adl.rm('/foldertodelete', recursive=True)

# print('Usage:', adl.du('', deep=True, total=True))  # total bytes usage
# print(adl.df(''))
# rpath = 'mynewfolder/*.txt'
# remote_name = '*.txt'
# remote_path = 'mynewfolder'
# lpath = ''
#
# pattern = remote_name.replace('*', '[0-9a-zA-Z\_]*').replace('?', '[0-9a-zA-Z\_]').replace('.', '\.')
# for child_path in adl.ls(remote_path):
#     child_filename = child_path.split('/')[-1]
#     target_path = lpath if '.' in rpath else child_filename if lpath == '' else lpath + child_filename if lpath.endswith(
#         '/') else '/'.join(
#         (lpath, child_filename))
#     if re.match(pattern, child_filename):
#         print('child_path:', child_path)
#         ADLDownloader(adl, lpath=target_path, rpath=child_path, overwrite=True)
#         print('{remote_path} has been downloaded to {local_path}.'.format(remote_path=child_path,
#                                                                           local_path=target_path))
