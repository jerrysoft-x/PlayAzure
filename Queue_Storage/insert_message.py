from azure.storage.queue import QueueService

queue_service = QueueService(account_name='mybiccstorage',
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

# queue_service.put_message('testqueue', u'Hello World')
# queue_service.put_message('testqueue', u'test message 1')
# queue_service.put_message('testqueue', u'test message 2')
# queue_service.put_message('testqueue', u'test message 3')

queue_service.put_message('testqueue', u'test message a')
queue_service.put_message('testqueue', u'test message b')
queue_service.put_message('testqueue', u'test message c')
