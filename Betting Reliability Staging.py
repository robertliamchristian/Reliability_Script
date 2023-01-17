# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 14:41:12 2022

@author: robbi
"""
from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup as bs4
import csv
from pandasql import sqldf
import requests
import numpy as np
pysqldf = lambda q: sqldf(q, globals())

#Load Team ID Join table
teamIDjoin = pd.read_csv("teamidjoin.csv")


#Load League Standings + Weight
#NBA
url_nba = 'https://www.espn.com/nba/standings/_/group/league'
page_nba = requests.get(url_nba)
soup_nba = bs4(page_nba.text, 'html.parser',store_line_numbers=False)

for tag_nba in soup_nba.find("th", class_='tar subHeader__item--content Table__TH'):
    tag_nba.append("Teams")

    table1_nba = soup_nba.find_all('table', class_='Table Table--align-right Table--fixed Table--fixed-left')
    nbatb1 = pd.read_html(str(table1_nba),header = None)[0]

    table2_nba = soup_nba.find_all('table', class_='Table Table--align-right')
    nbatb2 = pd.read_html(str(table2_nba),header=0, flavor = 'bs4')[0]

    nba_standings = pd.concat([nbatb1, nbatb2], axis=1) 
    nba_standings['Weight'] = np.linspace(1,0,30)
    nba_standings['Teams'] = nba_standings.Teams.str.extract('([A-Z_]+)')
    nba_standings['Team_ID'] = nba_standings['Teams'].str[:3]
    nba_standings.drop(nba_standings.columns[[4,5,6,7,8,9,10,11,12,13]],axis=1,inplace = True)
    nba_standings['League'] = 'NBA'
   # nba_standings.to_csv(r'nba_standings.csv', index=False)


#Scrape Odds and Last 10 Data per team

#Hawks
driver = webdriver.Chrome(executable_path=r'chromedriver.exe')
driver.get("https://www.oddsportal.com/search/results/:xAO4gBas/")

html = driver.page_source

tables = pd.read_html(html)
data = tables[0]
data1 = data.drop(data.columns[[1,6]],axis=1)
hawks_columns ={data.columns[0]: 'Date',
    data.columns[2]: 'Fixture',
    data.columns[3]: 'Results',
    data.columns[4]: 'Home_Odds',
    data.columns[5]: 'Away_Odds'}
data_hawks = data1.rename(columns = hawks_columns)

data_hawks['Home_PTS'] = data_hawks['Results'].str.split(':').str[0]
data_hawks['Away_PTS'] = data_hawks['Results'].str.split(':').str[-1]
data_hawks['Away_PTS'] = data_hawks['Away_PTS'].str.replace('[\Wa-zA-Z]', '')
data_hawks['Home_PTS'] = data_hawks['Home_PTS'].str.replace('[\Wa-zA-Z]', '')
data_hawks['Team_ID'] = 'ATL' #Team ID Changes each team
data_hawks['Home'] = data_hawks['Fixture'].str.split('-').str[0].str.strip()
data_hawks['Away'] = data_hawks['Fixture'].str.split('-').str[-1].str.strip()
data_hawks['Is_Away'] = np.where(data_hawks['Away']!= 'Atlanta Hawks', 0,1)


#Read W/D/L Weights Table
w_d_l_weights = pd.read_csv('CWeights.csv')

#Create Join and Calculated Columns
q = """

with W_D_L as (

        select b.*
            ,case when b.Home_Odds < b.Away_Odds and b.Is_Away = 0 then 1
                    when b.Home_Odds > b.Away_Odds and b.Is_Away = 1 then 1     
                    else 0 end as Is_Favorite
            ,th.betting_id as Home_ID
            ,ta.betting_id as Away_ID     
            ,sh.Weight as Home_Weight
            ,sa.Weight as Away_Weight
            
            ,case when b.Home_PTS < b.Away_PTS and b.Is_Away = 1 and sh.Weight > sa.Weight then 'A W +'
                    when b.Home_PTS > b.Away_PTS and b.Is_Away = 0 and sh.Weight < sa.Weight then 'H W +'
                    when b.Home_PTS < b.Away_PTS and b.Is_Away = 1 and sh.Weight < sa.Weight then 'A W -'
                    when b.Home_PTS > b.Away_PTS and b.Is_Away = 0 and sh.Weight > sa.Weight then 'H W -'
                    when b.Home_PTS = b.Away_PTS and b.Is_Away = 1 and sh.Weight < sa.Weight then 'A D +'
                    when b.Home_PTS = b.Away_PTS and b.Is_Away = 0 and sh.Weight < sa.Weight then 'H D +'
                    when b.Home_PTS = b.Away_PTS and b.Is_Away = 1 and sh.Weight < sa.Weight then 'A D -'
                    when b.Home_PTS = b.Away_PTS and b.Is_Away = 0 and sh.Weight > sa.Weight then 'H D -'
                    when b.Home_PTS > b.Away_PTS and b.Is_Away = 1 and sh.Weight > sa.Weight then 'A L +'
                    when b.Home_PTS < b.Away_PTS and b.Is_Away = 0 and sh.Weight < sa.Weight then 'H L +'
                    when b.Home_PTS > b.Away_PTS and b.Is_Away = 1 and sh.Weight < sa.Weight then 'A L -'
                    when b.Home_PTS < b.Away_PTS and b.Is_Away = 0 and sh.Weight > sa.Weight then 'H L -'
                    else null end as W_D_L
                    
            ,case  when b.Home_Odds < b.Away_Odds and b.Is_Away = 0 and b.Home_PTS > b.Away_PTS then 1
                 when b.Home_Odds < b.Away_Odds and b.Is_Away = 0 and b.Home_PTS < b.Away_PTS then 0
                 when b.Home_Odds > b.Away_Odds and b.Is_Away = 0 and b.Home_PTS > b.Away_PTS then 0
                 when b.Home_Odds > b.Away_Odds and b.Is_Away = 0 and b.Home_PTS < b.Away_PTS then 1
             
                when b.Home_Odds > b.Away_Odds and b.Is_Away = 1 and b.Home_PTS < b.Away_PTS then 1
                when b.Home_Odds > b.Away_Odds and b.Is_Away = 1 and b.Home_PTS > b.Away_PTS then 0
                when b.Home_Odds < b.Away_Odds and b.Is_Away = 1 and b.Home_PTS < b.Away_PTS then 0
                when b.Home_Odds < b.Away_Odds and b.Is_Away = 1 and b.Home_PTS > b.Away_PTS then 1
                else null end as Volatility
                
       from data_hawks b
       left join teamIDjoin th
           on b.home = th.betting_id
       left join teamIDjoin ta
           on b.away = ta.betting_id
       left join nba_standings sh
           on th.standings_ID = sh.Team_ID
       left join nba_standings sa
           on ta.standings_ID = sa.Team_ID
    limit 10)
    
    select w.Team_ID
    ,sum(wdl.PTS) as Consistency
    ,sum(w.Volatility) as Volatility
    ,(abs(sum(wdl.PTS)) + sum(w.Volatility))  /2 * .1 as Reliability 
 
    from W_D_L w
    left join w_d_l_weights wdl 
        on w.W_D_L = wdl.result_type
        
    group by w.Team_ID
    
    
    /*
    select w.*
        ,wdl.PTS as Result_PTS
    from W_D_L w
    left join w_d_l_weights wdl 
        on w.W_D_L = wdl.result_type
       limit 10/*;"""

sqlhawks = pysqldf(q)
print(sqlhawks)




#sqlhawks.to_csv(r'deletethis3.csv',index=False)





driver.close()

