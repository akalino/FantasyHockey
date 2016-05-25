"""
Project Name: WebScrapers 
@author: alkal 
Created on: 4/23/16 at  11:58 AM 
"""

import pandas as pd
import datetime
import re
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup


def get_number(x):
    if x == '--':
        x = 0
    return x


def get_name(x):
    try:
        z = re.search('<[tag][^>]*>(.+?)</[tag]>', x).group(1)
    except:
        z = ''
    return z


def flag_empty(x):
    if x == '':
        f = 1
    else:
        f = 0
    return f


def clean_data(team_data):
    team_data= str(team_data)
    team_soup = BeautifulSoup(team_data, 'lxml')

    tag_lst = team_soup.findAll('td')
    out_info = []
    for tag in tag_lst:
        x = re.search('<td>(.*)</td>', str(tag))
        if x is not None:
            out_info.append(x.group(1))

    # Remove headers
    player_info = out_info[8:]
    # Delete coach
    del player_info[-1]

    # Check for all fields
    print '*** All checks pass ***' if (len(player_info) % 8) == 0 else '*** Process failed ***'

    i=0
    new_list=[]
    while i<len(player_info):
        new_list.append(player_info[i:i+8])
        i+=8

    player_df = pd.DataFrame(new_list)
    player_df.columns = ['number', 'name', 'age', 'height', 'weight', 'shot', 'birthplace', 'birthday']
    player_df['number'] = player_df['number'].apply(get_number)
    player_df['name'] = player_df['name'].apply(get_name)
    player_df['pos'] = player_df['name'].apply(flag_empty)

    pos_list = ['C', 'LW', 'RW', 'D', 'G']
    assign_pos = []
    for i in player_df['pos']:
        if i == 0:
            assign_pos.append(pos_list[0])
        else:
            assign_pos.append(pos_list[0])
            del pos_list[0]

    player_df['pos'] = assign_pos
    #Removes the blank player lines
    player_df = player_df[player_df.name != '']
    player_df['team'] = city
    cur_date = datetime.date.today().strftime('%Y-%m-%d')
    player_df['updated'] = cur_date
    return player_df


def scrape(city):
    baseUrl = 'http://espn.go.com/nhl/team/roster/_/name/{city}'
    cur_url = (baseUrl.format(city=city))
    driver = webdriver.Chrome()
    driver.set_page_load_timeout(30)
    try:
        driver.get(cur_url)
    except TimeoutException:
        print '*** Skipped {city} ***'.format(city=city)
        driver.close()

    source = driver.page_source
    soup = BeautifulSoup(source, 'lxml')
    team_data = soup.findAll('div', {'class': 'mod-content'})
    team_data = team_data[0]
    driver.close()
    return team_data

def make_inserts(frame, table_name):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/hockey')
    conn = engine.connect()
    out_list = frame.to_dict(orient='records')
    metadata = sqlalchemy.schema.MetaData(bind=engine,reflect=True)
    table = sqlalchemy.Table(table_name, metadata, autoload=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    # bulk insert
    conn.execute(table.insert(), out_list)
    # Commit the changes
    session.commit()
    # Close the session
    session.close()


if __name__ == "__main__":

    # Put the argparser here

    print('*** Starting up ... ***')
    engine = create_engine('postgresql://postgres:password@localhost:5432/hockey')
    short_names = ['chi', 'col', 'dal', 'min', 'nsh', 'stl', 'wpg',           # Central Division
               'ana', 'ari', 'cgy', 'edm', 'los', 'san', 'van',           # Pacific Division
                'bos', 'buf', 'det', 'fla', 'mon', 'ott', 'tam', 'tor',   # Atlantic Division
                'car', 'cls', 'njd', 'nyi', 'nyr', 'phi', 'pit', 'was']   # Metropolitan Division

    for city in short_names:
        # TODO: some try catch for errors in the scrape
        td = scrape(city)
        df = clean_data(td)
        make_inserts(df, 'roster_players')


