import requests
import utils
import csv

twse_utl = "https://www.twse.com.tw"

def get_stock_day_all(today:str):
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    with requests.get(url) as r:
        with open(f"{today}/twse/STOCK_DAY_ALL.json", "wb") as f:
            f.write(r.content)
        data = r.json()
        with open(f'{today}/twse/STOCK_DAY_ALL.csv', 'w', newline='', encoding='UTF-8') as f:
            writer = csv.writer(f)
            writer.writerow(data["fields"])
            for stock in data["data"]:
                stock = [col.replace(',', '') for col in stock]
                writer.writerow(stock)

if __name__ == "__main__":
    today = utils.get_today()
    utils.create_dir(today)
    # task_list = [["exchangeReport/STOCK_DAY_ALL","STOCK_DAY_ALL"]]
    # for task in task_list:
    #     get_file_openapi(today,task)