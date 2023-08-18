import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import os
import sys
sys.path.append(r'C:\Users\ANDY\selfmade_python_module')
import sql_func
from tqdm.auto import tqdm
import time
import random


if __name__ == '__main__':
    y = '2023'

    opt_path = rf'C:\Users\ANDY\Documents\股市資料\資料\期交所_每秒成交資料_2018-now\選擇權\歷史每秒資料_rpt\{y}'
    taiex5_path = f'證交所_5秒大盤指數資料_2023-now/{y}'


    if not os.path.exists(taiex5_path):
        os.makedirs(taiex5_path)

    month_list = os.listdir(opt_path)

    for m in month_list:
        opt_path_m = os.path.join(opt_path,m)
        for date in tqdm(os.listdir(opt_path_m)):
            day = date[-6:-4]
            
            store_path = os.path.join(taiex5_path,f'{m}_{day}.db')

            if not os.path.exists(store_path):
                time.sleep(random.random()*8)
                data = []
                url = f'https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_INDEX?date={y}{m}{day}&response=html'

                html = requests.get(url)
                soup = bs(html.text,'lxml')

                trs = soup.tbody.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')[:2]
                    data.append([i.getText().replace(',','') for i in tds])
                df = pd.DataFrame(data,columns=['Time','Price'])
                sql_func.store_data(store_path,df,table_name='taiex')
        time.sleep(10.47393)