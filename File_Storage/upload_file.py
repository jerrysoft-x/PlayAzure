from azure.storage.file import FileService
from azure.storage.file import ContentSettings

file_service = FileService(account_name='mybiccstorage', account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

# file_service.create_share('testsharejerry')

file_service.create_file_from_path(
    'testsharejerry',
    None, # We want to create this blob in the root directory, so we specify None for the directory_name
    '20170301135604.png',
    '20170301135604.png',
    content_settings=ContentSettings(content_type='image/png'))