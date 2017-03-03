from azure.storage.queue import QueueService

queue_service = QueueService(account_name='mybiccstorage',
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

metadata = queue_service.get_queue_metadata('testqueue')
count = metadata.approximate_message_count
print('count = ', count)

