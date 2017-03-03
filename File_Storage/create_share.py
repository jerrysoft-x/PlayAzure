from azure.storage.file import FileService

file_service = FileService(account_name='mybiccstorage', account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

file_service.create_share('testsharejerry')