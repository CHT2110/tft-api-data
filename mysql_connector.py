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

    return mysql_connection

def create_drop_schema(connection, cursor):
    # Dropping existing schema to delete outdated data.
    # When patching happens we aren't interested in outdated data, and this saves storage.
    cursor.execute('DROP SCHEMA game_database')

    # Re-creating the schema
    cursor.execute('CREATE SCHEMA game_database')

    cursor.execute('USE game_database')

    connection.commit()

def create_tables(connection, cursor):
    # Creating tables for data

    cursor.execute('''CREATE TABLE IF NOT EXISTS summoners (
        summoner_id varchar(200) primary key,
        summoner_name varchar(200),
        league_points int,
        tier varchar(50),
        wins int,
        losses int,
        veteran boolean,
        inactive boolean,
        fresh_blood boolean,
        hot_streak boolean
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS player_info (
        summoner_id varchar(200) primary key,
        account_id varchar(200),
        puu_id varchar(200),
        summoner_name varchar(200),
        profile_icon_id int,
        revision_date bigint,
        summoner_level int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS meta_data (
        match_id varchar(200) primary key,
        participant_1 varchar(200),
        participant_2 varchar(200),
        participant_3 varchar(200),
        participant_4 varchar(200),
        participant_5 varchar(200),
        participant_6 varchar(200),
        participant_7 varchar(200),
        participant_8 varchar(200),
        game_datetime bigint,
        game_length int,
        data_version int
    );''')    

    cursor.execute('''CREATE TABLE IF NOT EXISTS participants (
        participant_id int auto_increment primary key,
        puuid varchar(250),
        match_id varchar(200),
        gold_left int,
        last_round int,
        level int,
        placement int,
        players_eliminated int,
        time_eliminated int,
        total_damage_to_players int
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS traits (
        id int auto_increment primary key,
        puuid varchar(250),
        name varchar(200),
        num_units int,
        style int,
        tier_current int,
        tier_total int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS units (
        id int auto_increment primary key,
        puuid varchar(250),
        character_id varchar(200),
        itemnames text,
        name varchar(200),
        rarity int,
        tier int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS companions (
        companion_id int auto_increment primary key,
        participant_id int,
        puuid varchar(250),
        content_id varchar(200),
        item_id int,
        skin_id int,
        species varchar(200)
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS augments (
        id int auto_increment primary key,
        puuid varchar(250),
        augment_name varchar(200)
    );''')

    connection.commit()

def ingest_summoner_data(summoner_data, connection, cursor):
    
    for summoner in summoner_data:
        summoner_id = summoner["summonerId"]
        summoner_name = summoner["summonerName"]
        league_points = summoner["leaguePoints"]
        tier = summoner["rank"]
        wins = summoner["wins"]
        losses = summoner["losses"]
        veteran = summoner["veteran"]
        inactive = summoner["inactive"]
        fresh_blood = summoner["freshBlood"]
        hot_streak = summoner["hotStreak"]

        # Insert data into the summoners table
        cursor.execute('''INSERT INTO summoners 
                            (summoner_id, summoner_name, league_points, tier, wins, losses, veteran, inactive, fresh_blood, hot_streak) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                        (summoner_id, summoner_name, league_points, tier, wins, losses, veteran, inactive, fresh_blood, hot_streak))

    connection.commit()

def ingest_player_info(player_data, connection, cursor):
    
    for data in player_data:
        summoner_id = data["id"]
        account_id = data["accountId"]
        puu_id = data["puuid"]
        summoner_name = data["name"]
        profile_icon_id = data["profileIconId"]
        revision_date = data["revisionDate"]
        summoner_level = data["summonerLevel"]

        cursor.execute('''INSERT INTO player_info 
                          (summoner_id, account_id, puu_id, summoner_name, profile_icon_id, revision_date, summoner_level) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                       (summoner_id, account_id, puu_id, summoner_name, profile_icon_id, revision_date, summoner_level))

        connection.commit()

def ingest_metadata(match_data, connection, cursor):
    for match in match_data:
        # Extract data from the match object
        metadata = match['metadata']
        data_version = metadata['data_version']
        match_id = metadata['match_id']
        participants = metadata['participants']
        # Extract data from the info object
        info = match['info']
        game_datetime = info['game_datetime']
        game_length = int(info['game_length'])
        # Insert data into the MySQL table
        query = '''
            INSERT INTO meta_data 
            (data_version, match_id, participant_1, participant_2, participant_3, participant_4, participant_5, participant_6, participant_7, participant_8, game_datetime, game_length) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (data_version, match_id, *participants, game_datetime, game_length)
        cursor.execute(query, values)
    
    connection.commit()


def ingest_participant_data(match_data, connection, cursor):
    for match in match_data:
        # Extract data from the participant object
        metadata = match['metadata']
        match_id = metadata['match_id']

        info = match['info']
        participants = info['participants']

        for participant in participants:
            puuid = participant['puuid']
            gold_left = participant['gold_left']
            last_round = participant['last_round']
            level = participant['level']
            placement = participant['placement']
            players_eliminated = participant['players_eliminated']
            time_eliminated = int(participant['time_eliminated'])
            total_damage_to_players = participant['total_damage_to_players']

            # Insert data into the MySQL table
            query = '''
                INSERT INTO participants 
                (puuid, match_id, gold_left, last_round, level, placement, players_eliminated, time_eliminated, total_damage_to_players) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (puuid, match_id, gold_left, last_round, level, placement, players_eliminated, time_eliminated, total_damage_to_players)
            cursor.execute(query, values)


    # Commit the changes
    connection.commit()

