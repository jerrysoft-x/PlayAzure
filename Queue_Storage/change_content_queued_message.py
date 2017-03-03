from azure.storage.queue import QueueService

queue_service = QueueService(account_name='mybiccstorage',
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

messages = queue_service.get_messages('testqueue')
for message in messages:
    queue_service.update_message('testqueue', message.id, message.pop_receipt, 0, u'Hello World Again')