from psycopg2 import sql, pool
import sqlalchemy
import pandas as pd
import credentials.postgres as cred
import credentials.redshift as cr
import logging
import datetime as datetime

# Logger Configuration
today_date = datetime.datetime.now().strftime(f'%Y-%m-%d')
log_filename = f'logs/log_{today_date}.log'
logging.basicConfig(filename=log_filename, 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info('''
             SCRIPT STARTED...
             ''')
try:
    class Postgres:
        # Create a simple connection pool with min and max connections (adjust numbers as needed)
        _connection_pool = pool.SimpleConnectionPool(1, 10,
                                                    host=cred.hostname,
                                                    dbname=cred.database,
                                                    user=cred.username,
                                                    password=cred.pas,
                                                    port=cred.port_id)

        @staticmethod
        def get_connection():
            try:    
                logging.info("Getting a Postgres connection from the connection pool")
                return Postgres._connection_pool.getconn()
            except Exception as e:
                logging.error("Error while connecting to Postgres from the connection pool: {0}".format(e))

        @staticmethod
        def release_connection(conn):
            try:    
                Postgres._connection_pool.putconn(conn)
                logging.info("Releasing the Postgres connection back to the connection pool")
            except Exception as e:
                logging.error("Error while realising the Postgres back to connection pool: {0}".format(e))

        @staticmethod
        def get_table_list():
            conn = Postgres.get_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT table_name FROM information_schema.tables")
                table_list = cur.fetchall()
                #cleaning the resulting list
                clean_list = [tupla[0].strip("()''") for tupla in table_list]
                logging.info("Postgres Table List retrieved correctly")
                return clean_list
            except Exception as e:
                logging.error("Error while fetching Postgres Table List: {0}".format(e))
            finally:
                cur.close()
                Postgres.release_connection(conn)
                logging.info("Releasing the Postgres connection back to the connection pool")

        @staticmethod
        def get_table_structure():
            '''importing all the table structure of the table schema "public" '''
            conn = Postgres.get_connection()
            cur = conn.cursor()
            try:
                cur.execute("""
                SELECT table_name, column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
                            """)
                table_schema = cur.fetchall()
                logging.info("Postgres Table Structure retrieved correctly (1/2)")
                #need to manually import the column name
                col_name_table_struc = ['table_name',
                                        'column_name',
                                        'data_type',
                                        'character_maximum_length'
                                        ]
                df = pd.DataFrame(table_schema, columns=col_name_table_struc)
                logging.info("Postgres Table Structure retrieved correctly (2/2)")
                return df
            except Exception as e:
                logging.error("Error while fetching Postgres Table List: {0}".format(e))
            finally:
                cur.close()
                Postgres.release_connection(conn)
                logging.info("Releasing the Postgres connection back to the connection pool.")

        @staticmethod
        def get_table(table_name):
            try:
                table_schema = "public" #default schema for Tableau Server
                conn = Postgres.get_connection()
                cur = conn.cursor()
                logging.info("Getting a Postgres connection from get_connection()")
                #Run a query to select all data from the specified table using the table name provided as a parameter.
                cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
                table = cur.fetchall()
                logging.info('For the table "{0}", content retrieved (1/2)'.format(table_name))
                #This second query retrieve the columns name of the table
                #cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s", (table_name))
                cur.execute('''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{0}'
                AND table_schema =  '{1}';
                '''.format(table_name, table_schema)
                )
                #cur.execute(sql.SQL("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {}").format(sql.Identifier(table_name)))
                columns = cur.fetchall()
                logging.info('For the table "{0}", column names retrieved (2/2)'.format(table_name))
                clean_list = [tupla[0].strip("()''") for tupla in columns]
            
                df = pd.DataFrame(table, columns=clean_list)
                logging.info('Table "{0}", retrieved from Postgres'.format(table_name))
                return df
            
            except Exception as error:
                logging.error("Error while fetching table: {0}".format({error}))
            finally:
                if cur:
                    cur.close()
                Postgres.release_connection(conn)

    class Redshift:
        _engine = None

        @staticmethod
        def connection_to_aws():
            try:    
                if not Redshift._engine:
                    #connection_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
                    connection_string = "postgresql://{0}:{1}@{2}:{3}/{4}?client_encoding=utf8".format(    
                        cr.redshift_username,
                        cr.redshift_password,
                        cr.redshift_endpoint,
                        cr.redshift_port,
                        cr.redshift_database
                    )
                    Redshift._engine = sqlalchemy.create_engine(connection_string)
                    logging.info("Redshift connection opened.")  
                return Redshift._engine
            except Exception as e:
                logging.error("Error while connecting to RedShift: {0}".format(e))   

        @staticmethod
        def table_to_aws(df, schema_name, table_name, column_types=None):
            '''
            to guarantee reusability, we created one connection 
            (the function above) and then, we pass all the tables
                df = table that must be copied
                schema_name = redshift schema
                table_name = name of the table
                column_types = structure of the table
            '''
            engine = Redshift.connection_to_aws()
            #logging.info('Redshift connection successful.')
            try:
                with engine.connect():
                    df.to_sql(name=table_name,
                            dtype=column_types,
                            con=engine,
                            schema=schema_name,
                            index=False,
                            if_exists='replace',
                            chunksize=10000,
                            method='multi'
                            )
                    logging.info(f"Table: {table_name} successfully uploaded to Redshift.")
            except Exception as e:
                logging.error(f"Error while uploading table to Redshift: {str(e)}")

        @staticmethod
        def execute_sql_command(sql_command):
            '''Executes the given SQL command on the Redshift database'''
            engine = Redshift.connection_to_aws()
            try:
                with engine.connect() as connection:
                    result = connection.execute(sql_command)
                    logging.info(f"SQL command executed successfully: {sql_command}")
                    return result
            except Exception as e:
                logging.error(f"Error while executing SQL command: {str(e)}")
        
        @staticmethod
        def close_connection():
            """
            Closes the Redshift database connection if it's open.
            """
            if Redshift._engine:
                Redshift._engine.dispose()
                Redshift._engine = None
                logging.info("Redshift connection closed.")

finally:
    logging.shutdown()