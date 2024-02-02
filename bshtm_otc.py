import requests
import logging
import utils
import twse
import os
import pandas as pd
import time
import csv
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
def get_stock(today):
    ok_list = []
    stock_list = []

    for file in os.listdir(f'{today}/bshtm_otc'):
        filename_without_extension = os.path.splitext(file)[0]
        ok_list.append(filename_without_extension)

    otc_csv = f'{today}/twse/OTC_DAY_ALL.csv'
    if not os.path.isfile(otc_csv):
        twse.get_otc_day_all(today)
    df = pd.read_csv(otc_csv)
    filtered_df = df[df.iloc[:, 9] != 0] # filter out no trade stock
    stock_list = filtered_df.iloc[:, 0].tolist()

    stock_list = [item for item in stock_list if item not in ok_list]
    
    return stock_list

def create_session():
    headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-TW,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://www.tpex.org.tw',
    'DNT': '1',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1'}
    session = requests.session()
    session.headers = headers
    session.verify = False
    return session



if __name__ == '__main__':
    today = utils.get_today()
    utils.create_dir(today)
    logging.basicConfig(filename=f'{today}/bshtm.log', level=logging.INFO,format='%(asctime)s | %(levelname)8s | %(message)s')
    logging.info("Crawler start")
    url = 'https://210.63.162.130/web/stock/aftertrading/broker_trading/download_ALLCSV.php'
    s = create_session()

    try:
        stock_list = get_stock(today)
    except Exception as e:
        logging.error(f'Get today stock Fail\t{e}')
        logging.error(f'Crawler Fail')
        os._exit(0)

    if len(stock_list)==0:
        logging.info(f'All stocks had downloaded')
    else:
        for stock in stock_list:
            payload = f"stk_code={stock}&charset=UTF-8"
            fail_flag = 0
            while 1:
                if fail_flag:
                    time.sleep(1)
                try:
                    with s.post(url,data=payload) as resp:
                        data = resp.content.decode("utf-8")
                        data = data.replace('"', '').encode("utf-8")
                        with open(f'{today}/bshtm_otc/{stock}.csv', 'wb') as f:
                            f.write(data)
                        with open(f'{today}/bshtm_otc/{stock}.csv') as csvfile:
                            csv_reader = csv.reader(csvfile)
                            for row in csv_reader:
                                if len(row[0]) > 12:
                                    raise Exception("content is broken")
                                break
                        logging.info(f'Get {stock} OK')
                        break

                except Exception as e:
                    logging.error(f'{stock} {e}')
                    fail_flag = 1
                    continue
    
    logging.info(f'Crawler Finish')

