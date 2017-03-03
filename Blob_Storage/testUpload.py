from azure.storage.blob import BlockBlobService

block_blob_service = BlockBlobService(account_name='mybiccstorage', account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')
#block_blob_service.create_blob_from_path('ot-test','myTestBlob',r'C:\Users\JJLI\Documents\JerryLee\Laboratory\DumpScripts.txt')

#generator = block_blob_service.list_blobs('ot-test')
#for blob in generator:
#    print(blob.name)

block_blob_service.get_blob_to_path('ot-test', 'myTestBlob', r'C:\Users\JJLI\Documents\JerryLee\Laboratory\out-DumpScripts.txt')