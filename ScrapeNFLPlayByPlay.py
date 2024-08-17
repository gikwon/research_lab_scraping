# -*- coding: utf-8 -*-
'''
Author: Gihyeon Kwon

Purpose: This program scrapes the play-by-play data for every NFL gmae starting
           in 1994.
'''

import os
import pandas as pd
import re
import requests
import sys
import time
from time import sleep
from bs4 import BeautifulSoup

start_time = time.time()
###############################################################################
###############################################################################
###############################################################################

# Set a few directories and file names
home = '/Users/gihyeonkwon/Documents/UCSD/Research/Tanner/Scraping'

rawdata = 'Data/Raw'
cleandata = 'Data/Clean'

code = 'Code'

outfile = 'NFLPlayByPlayRaw.csv'
playerout = 'NFLPlayerGameStats.csv'
starterout = 'NFLStarterCoachRaw.csv'



###############################################################################
###############################################################################
###############################################################################

# Break the program if the output file exists
if os.path.exists(os.path.join(home, rawdata, outfile)):
    print('Play output file already exists, delete if you want to re-scrape.')
    sys.exit(1)
    
if os.path.exists(os.path.join(home, rawdata, playerout)):
    print('Player output file already exists, delete if you want to re-scrape.')
    sys.exit(1)

if os.path.exists(os.path.join(home, rawdata, starterout)):
    print('Starter output file already exists, delete if you want to re-scrape.')
    sys.exit(1)

###############################################################################
#Helpers
def player_extract(soup, year, id):
    
    table = soup.find('table', id=id)

    data = []
    rows = table.find_all('tr')[2:]

    for row in rows:
        cells = row.find_all(['th', 'td'])
        row_data = [cell.get_text(strip=True) for cell in cells]
        player_link = cells[0].find('a', href=True)
        player_url = "https://www.pro-football-reference.com" + player_link['href'] if player_link else None
        row_data.insert(0, player_url)
    
        data.append(row_data)

    if id == "player_offense":
        columns=['playerurl', 'player', 'team', 
                'pass_cmp', 'pass_att', 'pass_yds',
                'pass_td', 'pass_int', 'pass_sacked',
                'pass_sacked_yds', 'pass_long',
                'pass_rating', 'rush_att',
                'rush_yds', 'rush_td', 'rush_long',
                'targets', 'rec', 'rec_yds',
                'rec_td', 'rec_long', 'fumbles_num', 'fumbles_fl']
    else:
        if year < 1999:
            columns=['playerurl', 'player', 'team', 
                    'def_int', 'def_int_yds', 'def_int_td',
                    'def_int_long', 'sacks',
                    'tackles_combined', 'tackles_solo',
                    'tackles_assists', 'fumbles_rec', 'fumbles_rec_yds',
                    'fumbles_rec_td', 'fumbles_forced']
        else:
            columns=['playerurl', 'player', 'team', 
                    'def_int', 'def_int_yds', 'def_int_td',
                    'def_int_long', 'pass_defended', 'sacks',
                    'tackles_combined', 'tackles_solo',
                    'tackles_assists', 'tackles_loss',
                    'qb_hits', 'fumbles_rec', 'fumbles_rec_yds',
                    'fumbles_rec_td', 'fumbles_forced']

    df = pd.DataFrame(data, columns=columns)
    return df
###############################################################################
    

# Loop to get each different year
#Finished 1994
#Finished ~2009
for year in range(2010, 2024):
    # Show where we are in the loop
    print(year)
    
    # Get the information from the webpage
    text = requests.get(r'https://www.pro-football-reference.com/years/{0}/games.htm'.format(year)).text
    
    # Get the link for each game in the year
    games = re.findall(r'\/boxscores\/\d+\w+\.\w+', text)
    
    # Loop over each game to get the information
    count = 0

    original_games = len(games)
    length_games = original_games
    for game in games:
        
        # Pull the information from the webpage
        text1 = requests.get(r'https://www.pro-football-reference.com' + game).text
        sleep(3)
        
        # Get the line and over-under.
        try:
            line = re.search(r'Vegas Line.*?>([^<]+)<\/td>', text1).group(1)
        except:
            line = None
        try:
            overunder = re.search(r'Over\/Under.*?>([\d\.]+)', text1).group(1)
        except:
            overunder = None
        try:
            homerec = re.findall(r'<div>(\d{1,2}-\d{1,2}(?:-\d{1,2})?)<\/div>', text1)[0]
            awayrec = re.findall(r'<div>(\d{1,2}-\d{1,2}(?:-\d{1,2})?)<\/div>', text1)[1]
        except:
            homerec = None
            awayrec = None
        
        
        # Get the title of game
        title = re.search(r'<title>([^<]+)', text1).group(1)

        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        # Play by Play

        # Clean the text for the play-by-play
        try:
            start = re.search(r'Start Time<\/strong>: ([^<]+)', text1).group(1)
        except:
            start = None
                
        text_play = text1.split('Full Play-By-Play')[-1]
        text_play = text_play.split('Schedules & Boxscores')[0]
        text_play = re.sub(r'<a.*?>|<\/a>', r'', text_play)
        text_play = re.sub(r'\s+', r' ', text_play)
        
        
        
        # Find each play and save to a DataFrame
        info = re.findall(r'scope="row".*?data-stat="quarter" >([^<]+)?.*?data-stat="qtr_time_remain" >([^<]+)?.*?data-stat="down" >([^<]+)?.*?data-stat="yds_to_go" >([^<]+)?.*?data-stat="location"(?: csk="\d+")? >([^<]+)?.*?data-stat="detail" >([^<]+)?.*?data-stat="pbp_score_aw" >([^<]+)?.*?data-stat="pbp_score_hm" >([^<]+)?.*?data-stat="exp_pts_before" >([^<]+)?.*?data-stat="exp_pts_after" >([^<]+)?.*?', text_play)

        temp = pd.DataFrame(info, columns=['quarter', 'time', 'down', 'togo',
                        'location', 'detail', 'det', 'pib', 'epb', 'epa'])
        
        temp['title'] = title
        temp['gametime'] = start
        
        temp['line'] = line
        temp['overunder'] = overunder
        
        temp['homerec'] = homerec
        temp['awayrec'] = awayrec

        #getting coin toss result
        match_details = re.findall(r'data-stat="detail"[^>]*>(.*?)</td>', text_play)
    
        ot_coin_result = ''
        for match in match_details[150:]:
            if "coin toss" in match:
                ot_coin_result = match
                break
            
        coin_row = pd.DataFrame({'detail': [ot_coin_result]})
        temp = pd.concat([temp, coin_row], ignore_index=True)

        # Save to the outfile
        try:
            temp.to_csv(os.path.join(home, rawdata, outfile), header=True, index=False, mode='a')
        except:
            sleep(.5)
            temp.to_csv(os.path.join(home, rawdata, outfile), header=True, index=False, mode='a')
        
        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        #Player info

        #Code
        text1 = re.sub('<!--|-->', '', text1)
        soup = BeautifulSoup(text1, 'html.parser')

        #offense
        offense = player_extract(soup, year, 'player_offense')

        #defense
        defense = player_extract(soup, year, 'player_defense')

        player_data = offense.merge(defense, how='outer', on=['playerurl', 'player', 'team'])
        player_data['title'] = title

        try:
            player_data.to_csv(os.path.join(home, rawdata, playerout), header=True, index=False, mode='a')
        except:
            sleep(.5)
            player_data.to_csv(os.path.join(home, rawdata, playerout), header=True, index=False, mode='a')
        
        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        #________________________________________________________________________________________________#
        #Starter and coach

        #Home team
        home_team = re.search(r'<title>([^<]+)', text1).group(1).split(" at ")[-1].split(' - ')[0].strip()
        #Away team
        away_team = re.search(r'<title>([^<]+)', text1).group(1).split(' at ')[0].strip()

        #Coach extract
        coaches = soup.find_all('a', href=True)

        coach_starter = pd.DataFrame(columns=["game", "home_starters", "home_starter_url", "coach", "coach_url"])

        coach_list = [[], []]
        home_starter_list = [[], []]
        away_starter_list = [[], []]

        count = 0
        for coach in coaches:
            if '/coaches/' in coach['href']:
                coach_list[0].append(coach.text)
                coach_list[1].append('https://www.pro-football-reference.com' + coach['href'])
                count += 1
                if count == 2:
                    break
        #Home starters extract
        home_starters = soup.find('table', id='home_starters')
        home_starters = home_starters.find_all('tr')

        for starters in home_starters:
            try:
                if '/players/' in starters.a['href']:
                    home_starter_list[0].append(starters.a.text)
                    home_starter_list[1].append('https://www.pro-football-reference.com' + starters.a['href'])
            except:
                pass
                
        #Away starters extract
        away_starters = soup.find('table', id='vis_starters')
        away_starters = away_starters.find_all('tr')

        num = 0
        for starters in away_starters:
            try:
                if '/players/' in starters.a['href']:
                    away_starter_list[0].append(starters.a.text)
                    away_starter_list[1].append('https://www.pro-football-reference.com' + starters.a['href'])
            except:
                pass
        
        #Putting to dfs and combining
        df_home_starters = pd.DataFrame({"starter": home_starter_list[0], 'starter_url': home_starter_list[1]})
        df_home_starters['team'] = home_team
        df_home_starters['coach'] = coach_list[0][1]
        df_home_starters['coach_url'] = coach_list[1][1]

        df_away_starters = pd.DataFrame({"starter": away_starter_list[0], 'starter_url': away_starter_list[1]})
        df_away_starters['team'] = away_team
        df_away_starters['coach'] = coach_list[0][0]
        df_away_starters['coach_url'] = coach_list[1][0]

        df_combined = pd.concat([df_home_starters, df_away_starters], ignore_index=True)
        df_combined['game_title'] = title  

        #Save the df to csv
        try:
            df_combined.to_csv(os.path.join(home, rawdata, starterout), header=True, index=False, mode='a')
        except:
            sleep(0.5)
            df_combined.to_csv(os.path.join(home, rawdata, starterout), header=True, index=False, mode='a')

        length_games-=1
        print(f'{original_games - length_games} out of {original_games} is done for {year}')
          
print("Done!")
print("Runtime:" ,time.time() - start_time)
            
