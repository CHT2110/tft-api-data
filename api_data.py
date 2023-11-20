import requests
import time
from tqdm import tqdm
from dotenv import load_dotenv
import os

# loading api_key from .env file
load_dotenv()

api_key = os.getenv('api_key')

# Start = 0 means we fetch count newest matches. 
start = '0'

# Count is the amount of mathces we want to fetch per player
count = '20'


# The following functions' purpose is to get the puu-id of summoners from Master - Challenger rank on the EU West server to get their match data. 

def all_master_rank_summoners():
    # api tft-league-v1
    url_master = 'https://euw1.api.riotgames.com/tft/league/v1/master?queue=RANKED_TFT' + '&api_key=' + api_key
    response_master = requests.get(url_master)
    player_info_master = response_master.json()
    summoner_master_data = player_info_master.get('entries', [])
    

    for entry in summoner_master_data:
        # renaming rank to identify master players
        entry['rank'] = 'master'
    
    return summoner_master_data

    '''
    Output:
            {
            "summonerId": "lgqyzaz7HhAIWKAP5AIJ05IQkC0NmDYbFfRaAjEdv2FPocg",
            "summonerName": "I am goodder",
            "leaguePoints": 779,
            "rank": "master",
            "wins": 98,
            "losses": 63,
            "veteran": false,
            "inactive": false,
            "freshBlood": true,
            "hotStreak": false
        }
    '''

def all_grandmaster_rank_summoners():
    # api tft-league-v1
    url_grandmaster = 'https://euw1.api.riotgames.com/tft/league/v1/grandmaster?queue=RANKED_TFT' + '&api_key=' + api_key
    response_grandmaster = requests.get(url_grandmaster)
    player_info_grandmaster = response_grandmaster.json()
    summonder_grandmaster_data = player_info_grandmaster.get('entries', [])

    for entry in summonder_grandmaster_data:
        # renaming rank to identify grandmaster players 
        entry['rank'] = 'grandmaster'

    return summonder_grandmaster_data

    '''
    Output:
            {
            "summonerId": "lgqyzaz7HhAIWKAP5AIJ05IQkC0NmDYbFfRaAjEdv2FPocg",
            "summonerName": "I am goodder",
            "leaguePoints": 779,
            "rank": "grandmaster",
            "wins": 98,
            "losses": 63,
            "veteran": false,
            "inactive": false,
            "freshBlood": true,
            "hotStreak": false
        }
    '''

def all_challenger_rank_summoner():
    # api tft-league.v1 
    url_challenger = 'https://euw1.api.riotgames.com/tft/league/v1/challenger?queue=RANKED_TFT' + '&api_key=' + api_key
    response_challenger = requests.get(url_challenger)
    player_info_challenger = response_challenger.json()
    summoner_challenger_data = player_info_challenger.get('entries', [])

    for entry in summoner_challenger_data:
        # renaming rank to identify challenger players
        entry['rank'] = 'challenger'

    return summoner_challenger_data

    '''
    Output:
            {
            "summonerId": "lgqyzaz7HhAIWKAP5AIJ05IQkC0NmDYbFfRaAjEdv2FPocg",
            "summonerName": "I am goodder",
            "leaguePoints": 779,
            "rank": "challenger",
            "wins": 98,
            "losses": 63,
            "veteran": false,
            "inactive": false,
            "freshBlood": true,
            "hotStreak": false
        }
    '''

def all_summoners():
    # Joining all summoner ids from master, grandmaster and challenger rank together
    summoner_data = (all_master_rank_summoners() +
                    all_grandmaster_rank_summoners() +
                    all_challenger_rank_summoner()
    )
    return summoner_data

def get_player_info(summoner_id):
    # Getting the player_info of a summoner. This contains the puu-id, which is the global id of player unlike summoner id.
    # API tft-summoner-v1
    url_player_info = f'https://euw1.api.riotgames.com/tft/summoner/v1/summoners/{summoner_id}' + '?api_key=' + api_key 
    response_player_info = requests.get(url_player_info)
    player_info = response_player_info.json()

    return player_info

    '''
    output:
            {
            "id": "lgqyzaz7HhAIWKAP5AIJ05IQkC0NmDYbFfRaAjEdv2FPocg",
            "accountId": "muRjLXzG607QJLQII3F2TzVgBQ-vJ_KGKlrAOO23nDTRp6o",
            "puuid": "eQUrCU42YEFk3EvrdACEirvVlhe4zT2kn9o2rBmSl7RLkmXPabL6Fkp4CULXTlSYssh4aGFTEKIukw",
            "name": "I am goodder",
            "profileIconId": 3398,
            "revisionDate": 1700329066000,
            "summonerLevel": 450
        }
    '''

def get_all_player_info():
    # Getting player info for all summoner_ids.
    summoner_data = all_summoners()
    summoner_ids = [entry['summonerId'] for entry in summoner_data]
    player_data = []

    for id in tqdm(summoner_ids, desc = 'Fetching player data'):
        player_info = get_player_info(id)
        player_data.append(player_info)
        
        # The Riot api only allows 100 requests every 2 minutes.
        time.sleep(1.2)

        # TODO This is just for testing since the for loop takes hours to complete due to the API's rate limit, and amount of ids. 
        if len(player_data) >= 100:
            break
    return player_data

# The following functions are to fetch the match data of the puu-ids. 

def get_match_id(puuid):
    # api tft-match-v1
    url_match_ids = f'https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}' + '/ids?start=' + start + '&count=' + count + '&api_key=' + api_key

    response_match_ids = requests.get(url_match_ids)
    match_ids = response_match_ids.json()

    return match_ids

    '''
    output:
            [
        "EUW1_6681384040",
        "EUW1_6681331522",
        "EUW1_6681284110",
        "EUW1_6668355988",
        "EUW1_6668281520",
        "EUW1_6668217716",
        "EUW1_6668150430"
    ]
    '''
def get_all_match_ids():
    player_data = get_all_player_info()
    puuids = [entry['puuid'] for entry in player_data]
    all_match_ids = []

    for id in tqdm(puuids, desc = 'Fetching match ids'):
        match_id = get_match_id(id)
        all_match_ids.extend(match_id)
        
        # The Riot api only allows 100 requests every 2 minutes.
        time.sleep(1.2)

        # TODO This is just for testing since the for loop takes hours to complete due to the API's rate limit, and amount of ids. 
        if len(all_match_ids) >= 100:
            break
    return all_match_ids

def get_match_data(match_id):
    url_match_data = 'https://europe.api.riotgames.com/tft/match/v1/matches/' + match_id + '?api_key=' + api_key

    response_match_data = requests.get(url_match_data)
    match_data = response_match_data.json()

    return match_data

def get_all_match_data():
    
    match_ids = get_all_match_ids()

    match_data = []

    for id in tqdm(match_ids, desc = 'Fetching match data'):
        fetch_match_data = get_match_data(id)
        match_data.append(fetch_match_data)

        # The Riot api only allows 100 requests every 2 minutes.
        time.sleep(1.2)

        # TODO This is just for testing since the for loop takes hours to complete due to the API's rate limit, and amount of ids. 
        if len(match_data) >= 100:
            break
    
    return match_data
    
    # The output is things like players in a match, their placement, traits, augments etc.  

