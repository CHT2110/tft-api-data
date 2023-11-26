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
    cursor.execute('DROP SCHEMA sources')

    # Re-creating the schema
    cursor.execute('CREATE SCHEMA sources')

    cursor.execute('USE sources')

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
        revision_date int,
        summoner_level int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS match_data (
        match_id VARCHAR(20) primary key,
        data_version VARCHAR(10),
        game_datetime BIGINT,
        game_length FLOAT,
        game_version VARCHAR(50)
    );''')    

    cursor.execute('''CREATE TABLE IF NOT EXISTS participants (
        puuid varchar(64) primary key,
        match_id varchar(20),
        gold_left int,
        last_round int,
        level int,
        placement int,
        players_eliminated int,
        time_eliminated float,
        total_damage_to_players int
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS traits (
        id int auto_increment primary key,
        puuid varchar(64),
        name varchar(50),
        num_units int,
        style int,
        tier_current int,
        tier_total int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS units (
        id int auto_increment primary key,
        puuid varchar(64),
        character_id varchar(20),
        itemnames text,
        name varchar(50),
        rarity int,
        tier int
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS companions (
        id int auto_increment primary key,
        puuid varchar(64),
        content_id varchar(36),
        item_id int,
        skin_id int,
        species varchar(50)
    );''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS augments (
        id int auto_increment primary key,
        puuid varchar(64),
        augment_name varchar(50)
    );''')

    connection.commit()

    cursor.close()

    connection.close()