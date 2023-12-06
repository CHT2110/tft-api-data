from dotenv import load_dotenv
import os
from api_data import *

# loading api_key from .env file
load_dotenv()

api_key = os.getenv('api_key')

# Start = 0 means we fetch count newest matches. 
start = '0'

# Count is the amount of mathces we want to fetch per player
count = '20'

def main():
    create_drop_schema(connection, cursor)
    create_tables(connection, cursor)
        
    all_summoners()
    get_all_player_info(cursor)
    get_all_match_data()
    
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()