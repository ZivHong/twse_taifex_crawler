import requests
import utils
import csv
import pandas as pd

twse_utl = "https://www.twse.com.tw"

def get_stock_day_all(today:str):
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    with requests.get(url) as r:
        with open(f"{today}/twse/STOCK_DAY_ALL.json", "wb") as f:
            f.write(r.content)
        data = r.json()
        title_list = data["fields"]
        csv_data = []
        for stock in data["data"]:
            csv_data.append([col.replace(',', '') for col in stock])
        df = pd.DataFrame(csv_data,columns=title_list) 
        df.to_csv(f'{today}/twse/STOCK_DAY_ALL.csv',index=False)

def get_otc_day_all(today:str):
    roc_today = utils.get_today(roc_era=True)
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?d={roc_today}&se=EW"
    title_list = ["代號", "名稱", "收盤","漲跌","開盤","最高","最低","成交股數","成交金額(元)","成交筆數","最後買價","最後買量(千股)","最後賣價","後賣量(千股)","發行股數","次日漲停價","次日跌停價"]
    csv_data = []
    with requests.get(url) as r:
        with open(f"{today}/twse/OTC_DAY_ALL.json", "wb") as f:
            f.write(r.content)
        data = r.json()
        for stock in data["aaData"]:
            csv_data.append([col.replace(',', '') for col in stock])
        df = pd.DataFrame(csv_data,columns=title_list) 
        df.to_csv(f'{today}/twse/OTC_DAY_ALL.csv',index=False)

if __name__ == "__main__":
    today = utils.get_today()
    utils.create_dir(today)
    get_otc_day_all(today)
    df = pd.read_csv(f"{today}/twse/STOCK_DAY_ALL.csv")
    print(df[df.iloc[:, 9] == 0])
    # task_list = [["exchangeReport/STOCK_DAY_ALL","STOCK_DAY_ALL"]]
    # for task in task_list:
    #     get_file_openapi(today,task)