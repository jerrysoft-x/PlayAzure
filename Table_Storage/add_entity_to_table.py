from azure.storage.table import TableService, Entity

table_service = TableService(account_name="mybiccstorage",
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

# table_service.create_table('testTableJerry')

task = {'PartitionKey': 'tasksSeattle', 'RowKey': '1', 'description': 'Take out the trash', 'priority': 200}
table_service.insert_entity('testTableJerry', task)
