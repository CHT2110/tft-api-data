import mysql.connector
import json

def connector():
    # loading confic.json for connector
    with open('config.json') as configuration:
        config = json.load(configuration)['mysql']
    
    # Setting up connection
    mysql_connection = mysql.connector.connect(
        host = config['host'],
        port = config['port'],
        user = config['user'],
        password = config['password'],
        database = config['database']
    )

    cursor = mysql_connection.cursor()

    return cursor

def create__drop_schema(connection, cursor):
    # Dropping existing schema to delete outdated data.
    # When patching happens we aren't interested in outdated data, and this saves storage.
    cursor.execute('DROP SCHEMA IF EXISTS source')

    # Re-creating the schema
    cursor.execute('CREATE SCHEMA sources')

    connection.commit()

def create_table(connection, cursor, data):
    # Creating dynamic columns in case riot decides to chance column namesS
    column_definitions = ', '.join([f'{key} VARCHAR(255) NOT NULL' for key in data[0].keys()])
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS source.summoners (
            {column_definitions}
        )
    """

    #Inserting data into the tables
    for summoner in data:
        column_names = ', '.join(summoner.keys())
        column_values = ', '.join([f"'{value}'" for value in summoner.values()])
        insert_query = f"INSERT INTO source.summoners ({column_names}) VALUES ({column_values})"
        cursor.execute(insert_query)

    connection.commit()