from bs4 import BeautifulSoup
import requests
import pandas as pd
from config import *
import mysql.connector

url= "https://www.speedtest.net/global-index"
page=requests.get(url)
soup = BeautifulSoup(page.text, 'lxml')

header= []

db = mysql.connector.connect(
    host=HOST,
    user=USERNAME,
    password=PASSWORD,
    database=DATABASE
)
cursor = db.cursor()

try:
    cursor.execute("CREATE TABLE mobile (`rank` INT(255), `rank_change` VARCHAR(255), `country` VARCHAR(255), `speed` FLOAT)")
except Exception as e:
    print(e)
try:
    cursor.execute("CREATE TABLE broadband (`rank` INT(255), `rank_change` VARCHAR(255), `country` VARCHAR(255), `speed` FLOAT)")
except Exception as e:
    print(e)


for i in range(2,4):
    table=soup.find_all('table')[i]
    tbody=table.find('tbody')
    tr=tbody.find_all('tr')
    # print(tr)

    result = []
    for tr in table.find_all('tr', class_="data-result results"):
        td = tr.find_all('td')
        data_list = []
        for data in td:
            x = data.text.strip()
            data_list.append(x)
        result.append(data_list)
        
    # print(result)

    df = pd.DataFrame(result, columns = ['rank', 'rank_change', 'country', 'speed'])
    # print(df)
    if i==2:
        file_name = "mobile.csv"
    else:
        file_name = "broadband.csv"
    
    df.to_csv(file_name,index=False)
    print(df)
    
    if i==2:
        table_name= "mobile"
    else:
        table_name = "broadband"

    for index, row in df.iterrows():
        query = f'''INSERT INTO {table_name} (`rank`,`rank_change`,`country`,`speed`) value ("{row['rank']}","{row['rank_change']}","{row['country']}","{row['speed']}")'''
        print(query)

        try:
            cursor.execute(query)
        except Exception as e:
            print(e)
    
db.commit()
