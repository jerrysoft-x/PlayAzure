from azure.storage.table import TableService, Entity, TableBatch

table_service = TableService(account_name="mybiccstorage",
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

# batch = TableBatch()
# task10 = {'PartitionKey': 'tasksSeattle', 'RowKey': '10', 'description' : 'Go grocery shopping', 'priority' : 400}
# task11 = {'PartitionKey': 'tasksSeattle', 'RowKey': '11', 'description' : 'Clean the bathroom', 'priority' : 100}
# batch.insert_entity(task10)
# batch.insert_entity(task11)
# table_service.commit_batch('testTableJerry', batch)

task12 = {'PartitionKey': 'tasksSeattle', 'RowKey': '14', 'description' : 'Go grocery shopping2', 'priority' : 400}
task13 = {'PartitionKey': 'tasksSeattle', 'RowKey': '15', 'description' : 'Clean the bathroom2', 'priority' : 100}

with table_service.batch('testTableJerry') as batch:
    batch.insert_entity(task12)
    batch.insert_entity(task13)