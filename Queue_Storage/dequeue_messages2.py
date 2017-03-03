from azure.storage.queue import QueueService

queue_service = QueueService(account_name='mybiccstorage',
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

messages = queue_service.get_messages('testqueue', num_messages = 32, visibility_timeout = 5 * 60)
for message in messages:
    print('message.content =', message.content, end = ' ')
    print('message.id =', message.id, end = ' ')
    print('message.pop_receipt =', message.pop_receipt)
    queue_service.delete_message('testqueue', message.id, message.pop_receipt)