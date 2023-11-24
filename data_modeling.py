import pandas as pd
from api_data import *

summoners = all_summoners()

summoners_df = pd.DataFrame(summoners)


player_info = get_all_player_info()

player_info_df = pd.DataFrame(player_info)

match_data = get_all_match_data()

match_data_df = pd.DataFrame(match_data)

