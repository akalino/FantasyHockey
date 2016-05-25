#! /home/alkal/anaconda/bin python
"""
Project Name: WebScrapers 
@author: alkal 
Created on: 4/23/16 at  11:24 PM 
"""

# Cron for daily_scores should run between 3-4 AM

import pandas as pd
import datetime
import re
import sqlalchemy
from string import digits
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup


def get_teams(x):
    y = re.search('<dd class="score">(.*)</dd>', str(x)).group(1).translate(None, digits).lower().split('-')
    z = [k.strip() for k in y]
    return z


def get_name(x):
    try:
        z = re.search('(?=data-entity-display-name=")(.*?)(?=" data-entity-id)', x).group(1)
    except:
        z = ''
    z = z.replace('\"', '').split('=')[1]
    return z


def get_sec(s):
    l = s.split(':')
    if len(l) == 1:
        t = l[0]
    elif len(l) == 2:
        t = int(l[0]) * 60 + int(l[1])
    elif len(l) == 3:
        t = int(l[0]) * 3600 + int(l[1]) * 60 + int(l[2])
    return t


def get_parsed_stats(x, stat_name):
    try:
        y = re.search('title="{stat_name}">(.*)</td>'.format(stat_name=stat_name), str(x)).group(1)
    except:
        y = None
    return y


def exists(it):
    return (it is not None)


def checkEquality(lst):
    return lst[1:] == lst[:-1]


def combine_goalie_stats(cur_url, player_name_list,
                         shots_against_list, goals_against_list,
                         saves_list, save_pct_list, goalie_toi_list):

    goalie_check = [len(shots_against_list), len(goals_against_list), len(saves_list),
                    len(save_pct_list), len(goalie_toi_list)]
    if checkEquality(goalie_check):
        print('*** Goalie stats processed for {game} ***'.format(game=cur_url))
    else:
        print('*** Goalie stats failed for {game} ***'.format(game=cur_url))

    goalie_names = player_name_list[:len(shots_against_list)]
    goalie_data = [goalie_names, shots_against_list, goals_against_list,
                       saves_list, save_pct_list, goalie_toi_list]
    goalie_stats = pd.DataFrame(goalie_data).transpose()
    goalie_stats.columns = ['name', 'shots_against', 'goals_against', 'saves', 'save_pct', 'toi']
    return goalie_stats


def combine_skater_stats(cur_url, player_name_list, shots_against_list,
                         goals_list, assis_list, points_list,
                         pm_list, pen_mins_list, sog_list,
                         blocks_list, hits_list, takes_list,
                         gives_list, fow_list, fol_list,
                         fop_list, shifts_list, skater_toi_list):

    skater_check = [len(goals_list), len(assis_list), len(points_list),
                    len(pm_list), len(pen_mins_list), len(sog_list),
                    len(blocks_list), len(hits_list), len(takes_list),
                    len(gives_list), len(fow_list), len(fol_list),
                    len(fop_list), len(shifts_list), len(skater_toi_list)]
    if checkEquality(skater_check):
        print('*** Skater stats processed for {game} ***'.format(game=cur_url))
    else:
        print('*** Skater stats failed for {game} ***'.format(game=cur_url))

    skater_names = player_name_list[len(shots_against_list):]
    skater_data = [skater_names, goals_list, assis_list, points_list,
                       pm_list, pen_mins_list, sog_list,
                       blocks_list, hits_list, takes_list,
                       gives_list, fow_list, fol_list,
                       fop_list, shifts_list, skater_toi_list]

    skater_stats = pd.DataFrame(skater_data).transpose()
    skater_stats.columns = ['name', 'goals', 'assists', 'points',
                            'plus_minus', 'penalty_minutes', 'sog',
                            'blocks', 'hits', 'takeaways', 'giveaways',
                            'fo_win', 'fo_lost', 'fo_pct',
                            'shifts', 'toi']
    return skater_stats


def fetch_player_ids():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/hockey')
    conn = engine.connect()
    player_query = '''SELECT player_id, name FROM roster_players'''
    player_index = pd.read_sql(player_query,conn)
    player_index['name'] = player_index['name'].astype(str)
    return player_index


def make_inserts(frame, table_name):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/hockey')
    conn = engine.connect()
    out_list = frame.to_dict(orient='records')
    metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
    table = sqlalchemy.Table(table_name, metadata, autoload=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    # bulk insert
    conn.execute(table.insert(), out_list)
    # Commit the changes
    session.commit()
    # Close the session
    session.close()


def run(baseUrl):
    cur_url = (baseUrl)
    driver = webdriver.Chrome()
    driver.set_page_load_timeout(30)
    try:
        driver.get(cur_url)
    except TimeoutException:
        print '*** Skipped the url: {url} ***'.format(url=baseUrl)
        driver.close()

    source = driver.page_source
    soup = BeautifulSoup(source, 'lxml')
    driver.close()

    score_data = soup.findAll('div',
                              {'class': 'yom-mod yom-app yom-sports-scoreboard yom-scores daily-fantasy-bridge nhl '})
    score_soup = BeautifulSoup(str(score_data[0]), 'lxml')
    homeUrl = 'http://sports.yahoo.com'
    get_game_urls = score_soup.findAll('tr', {'class': 'game link'})
    game_links = []
    for i in get_game_urls:
        cur_game = i
        link = re.search('data-url(.*)> (?=<td class="summary">)', str(cur_game)).group(1)
        link = link[2:-2]
        game_links.append(homeUrl + link)

    for i in game_links:
        cur_url = i
        driver = webdriver.Chrome()
        driver.set_page_load_timeout(30)
        try:
            driver.get(cur_url)
        except TimeoutException:
            print '*** Skipped game url: {game} ***'.format(game=cur_url)
            driver.close()

        source = driver.page_source
        soup = BeautifulSoup(source, 'lxml')
        driver.close()

        player_names = soup.find_all(attrs={"class":"athlete", "scope":"row"})
        player_name_list = [get_name(str(j)) for j in player_names]

        # goalie stats
        shots_against = soup.find_all(attrs={"title": "Shots Against"})
        shots_against_list = filter(exists, [get_parsed_stats(str(j), 'Shots Against') for j in shots_against])

        goals_against = soup.find_all(attrs={"title":" Goals Against"})
        goals_against_list = filter(exists, [get_parsed_stats(str(j), 'Goals Against') for j in goals_against])

        saves = soup.find_all(attrs={"title": "Saves"})
        saves_list = filter(exists, [get_parsed_stats(str(j), 'Saves') for j in saves])

        save_pct = soup.find_all(attrs={"title": "Save Percentage"})
        save_pct_list = filter(exists, [get_parsed_stats(str(j), 'Save Percentage') for j in save_pct])

        goalie_toi = soup.find_all(attrs={"title": "Time On Ice"})
        goalie_toi_list = filter(exists, [get_parsed_stats(str(j), 'Time On Ice') for j in goalie_toi])

        # skater stats
        goals = soup.find_all(attrs={"title": "Goals"})
        goals_list = filter(exists, [get_parsed_stats(str(j), 'Goals') for j in goals])

        assis = soup.find_all(attrs={"title": "Assists"})
        assis_list = filter(exists, [get_parsed_stats(str(j), 'Assists') for j in assis])

        points = soup.find_all(attrs={"title": "Points"})
        points_list = filter(exists, [get_parsed_stats(str(j), 'Points') for j in points])

        pm = soup.find_all(attrs={"title": "Plus Minus"})
        pm_list = filter(exists, [get_parsed_stats(str(j), 'Plus Minus') for j in pm])

        pen_mins = soup.find_all(attrs={"title": "Penalty Minutes"})
        pen_mins_list = filter(exists, [get_parsed_stats(str(j), 'Penalty Minutes') for j in pen_mins])

        sog = soup.find_all(attrs={"title": "Shots on Goal"})
        sog_list = filter(exists, [get_parsed_stats(str(j), 'Shots on Goal') for j in sog])

        blocks = soup.find_all(attrs={"title": "Blocks"})
        blocks_list = filter(exists, [get_parsed_stats(str(j), 'Blocks') for j in blocks])

        hits = soup.find_all(attrs={"title": "Hits"})
        hits_list = filter(exists, [get_parsed_stats(str(j), 'Hits') for j in hits])

        takes = soup.find_all(attrs={"title": "Takeaways"})
        takes_list = filter(exists, [get_parsed_stats(str(j), 'Takeaways') for j in takes])

        gives = soup.find_all(attrs={"title": "Giveaways"})
        gives_list = filter(exists, [get_parsed_stats(str(j), 'Giveaways') for j in gives])

        fow = soup.find_all(attrs={"title": "Faceoffs Won"})
        fow_list = filter(exists, [get_parsed_stats(str(j), 'Faceoffs Won') for j in fow])

        fol = soup.find_all(attrs={"title": "Faceoffs Lost"})
        fol_list = filter(exists, [get_parsed_stats(str(j), 'Faceoffs Lost') for j in fol])

        fop = soup.find_all(attrs={"title": "Faceoff Percentage"})
        fop_list = filter(exists, [get_parsed_stats(str(j), 'Faceoff Percentage') for j in fop])

        shifts = soup.find_all(attrs={"title": "Shifts"})
        shifts_list = filter(exists, [get_parsed_stats(str(j), 'Shifts') for j in shifts])

        skater_toi = soup.find_all(attrs={"title": "Time on Ice"})
        skater_toi_list = filter(exists, [get_parsed_stats(str(j), 'Time on Ice') for j in skater_toi])

        g = combine_goalie_stats(cur_url, player_name_list,
                                 shots_against_list, goals_against_list,
                                 saves_list, save_pct_list, goalie_toi_list)

        s = combine_skater_stats(cur_url, player_name_list, shots_against_list,
                                 goals_list, assis_list, points_list,
                                 pm_list, pen_mins_list, sog_list,
                                 blocks_list, hits_list, takes_list,
                                 gives_list, fow_list, fol_list,
                                 fop_list, shifts_list, skater_toi_list)

        p_idx = fetch_player_ids()

        skater_stats = pd.merge(p_idx, s, on='name')
        skater_stats['date_played'] = datetime.date.today().strftime('%Y-%m-%d')
        skater_stats['fo_pct'] = skater_stats['fo_pct'].replace('%', '', regex=True).astype('float')/100
        skater_stats['toi'] = skater_stats['toi'].apply(get_sec)

        goalie_stats = pd.merge(p_idx, g, on='name')
        goalie_stats['date_played'] = datetime.date.today().strftime('%Y-%m-%d')
        goalie_stats['toi'] = goalie_stats['toi'].apply(get_sec)

        make_inserts(skater_stats, 'daily_skater_stats')
        make_inserts(goalie_stats, 'daily_goalie_stats')


if __name__ == "__main__":
    # Put the argparser here
    print('*** Starting up ... ***')
    # The default url should be
    baseUrl = 'http://sports.yahoo.com/nhl/scoreboard'
    # A backfill url will look like
    # http://sports.yahoo.com/nhl/scoreboard/?date=2016-04-26&conf=
    run(baseUrl)
    print('*** Finished daily job ***')
