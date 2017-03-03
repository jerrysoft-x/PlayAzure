from azure.storage.table import TableService, Entity, TableBatch

table_service = TableService(account_name="mybiccstorage",
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

# tasks = table_service.query_entities('testTableJerry', filter="PartitionKey eq 'tasksSeattle'")
# customer
# tasks = table_service.query_entities('customer')
tasks = table_service.query_entities('ubicchdi30mar2016at061058155syslog')

for task in tasks:
    # print(task.description)
    # print(task.priority)
    print(task)
