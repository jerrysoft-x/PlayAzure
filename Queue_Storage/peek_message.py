from azure.storage.queue import QueueService

queue_service = QueueService(account_name='mybiccstorage',
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

messages = queue_service.peek_messages('testqueue', num_messages = 32)

print(len(messages))

for message in messages:
    print('message.content = ', message.content, end = ' ')
    print('message.dequeue_count = ', message.dequeue_count, end = ' ')
    print('message.expiration_time = ', message.expiration_time, end = ' ')
    print('message.id = ', message.id, end = ' ')
    print('message.insertion_time = ', message.insertion_time, end = ' ')
    print('message.pop_receipt = ', message.pop_receipt, end = ' ')
    print('message.time_next_visible = ', message.time_next_visible)
