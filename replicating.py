from main import Postgres, Redshift
import config as config
import credentials.redshift as cr
#import pandas as pd

#path = """C:\\\\Users\\\\sesa672202\\\\Documents\\\\TestPostgresRedshift\\\\test\\\\"""
schema_name = cr.redshift_schema_eur

### TABLES
mapping_list_table = config.mapping_list_tables
list_of_tables = config.list_of_tables
i = 0
for table in list_of_tables[i:len(list_of_tables)+1]:
    try:
        if table != "groups":
            df = Postgres.get_table(table)
            mapping_dict = mapping_list_table[i]
            Redshift.table_to_aws(df, schema_name, table, column_types=mapping_dict)
            i += 1
        else:
            df = Postgres.get_table(table)  
            df.rename(columns={"system": "system_"}, inplace=True)  #forbidden word!
            mapping_dict = mapping_list_table[i]
            Redshift.table_to_aws(df, schema_name, table, column_types=mapping_dict)
            i += 1
    except Exception as e:
        print(f'Error: {str(e)}')

#Grant the access to the selected users
Redshift.execute_sql_command('grant usage on schema <schema_name> to <username>;')
Redshift.execute_sql_command('grant select on ALL TABLES IN SCHEMA <schema_name> to <username>;')

#Close the connection
Redshift.close_connection()
print('end')