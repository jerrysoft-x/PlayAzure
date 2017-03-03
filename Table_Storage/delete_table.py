from azure.storage.table import TableService, Entity, TableBatch

table_service = TableService(account_name="mybiccstorage",
                             account_key='VdxZ3Gd6UN+I1iIWH78NSMehp72GUSJFkb6kSEyo2PXX98rfW+ePlMaPdaYcs+4MH8mRl+EaDT+6GcpIorwKgw==')

table_service.delete_table('testTableJerry')