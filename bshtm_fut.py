import requests
import asyncio
import aiohttp
import utils
import sys
import os
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

normal_url = "https://www.taifex.com.tw/cht/3/dailyFutures"
download_url = "https://www.taifex.com.tw/cht/3/dailyFuturesDown"
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"}
    
def check_file_exist(task_list):
    not_exist_list = []
    for future in task_list:
        del_flag = False
        if future[0] == "STF":
            filename = f"{output_dir}/{future[1]}_{future[2]}.csv"
        else:
            filename = f"{output_dir}/{future[0]}_{future[2]}.csv"

        if not os.path.isfile(filename):
            not_exist_list.append(future)
        else:
            try:
                with open(filename,"r") as f:
                    first_line = f.readline()
                    if "HTML" in first_line:
                        not_exist_list.append(future)
                        del_flag = True
            except UnicodeDecodeError:
                continue

        if del_flag: os.remove(filename)

    return not_exist_list

async def get_bshtm(info_pair,date,marketcode):
    if info_pair[0] == "STF":
        output_file = f"{output_dir}/{info_pair[1]}_{info_pair[2]}.csv"
    else:
        output_file = f"{output_dir}/{info_pair[0]}_{info_pair[2]}.csv"

    payload={"queryDate": date, "queryDateAh": date, 
         "commodityId": info_pair[0], "commodityId2": info_pair[1], "marketcode": marketcode, 
         "commodity_idt": info_pair[0], "commodity_id2t": info_pair[1], "MarketCode": marketcode, 
         "doQuery": "1", "settlemon": info_pair[2]}
    
    with requests.post(normal_url, headers=headers, data=payload) as resp:
        cookies = {k: v for k, v in resp.cookies.items()}
    async with aiohttp.request('POST', download_url, headers=headers, data=payload, cookies=cookies) as resp:
        with open(output_file, 'wb') as f:
            f.write(await resp.content.read())

async def init_bshtm(task_list,date,marketcode):
    tasks = [asyncio.create_task(get_bshtm(pair,date,marketcode)) for pair in task_list]
    await asyncio.gather(*tasks)

def get_bshtm(task,date,marketcode):
    if task[0] == "STF":
        output_file = f"{output_dir}/{task[1]}_{task[2]}.csv"
    else:
        output_file = f"{output_dir}/{task[0]}_{task[2]}.csv"

    payload={"queryDate": date, "queryDateAh": date, 
         "commodityId": task[0], "commodityId2": task[1], "marketcode": marketcode, 
         "commodity_idt": task[0], "commodity_id2t": task[1], "MarketCode": marketcode, 
         "doQuery": "1", "settlemon": task[2]}
    with requests.Session() as s:
        s.headers.update(headers)
        s.post(normal_url, data=payload)
        with s.post(download_url, data=payload) as resp:
            with open(output_file, 'wb') as f:
                f.write(resp.content)

def download_data(task_list,date,marketcode):
    task_list = check_file_exist(task_list)
    while len(task_list) != 0:
        # asyncio.run(init_bshtm(task_list,date,marketcode))
        # task_list = check_file_exist(task_list)
        tasks = [[task,date,marketcode] for task in task_list]
        with ThreadPoolExecutor(16) as executor:
            executor.map(get_bshtm, *zip(*tasks))
        task_list = check_file_exist(task_list)

def get_huge_bshtm(date,marketcode):
    huge_url = "https://www.taifex.com.tw/cht/3/dailyHugeDealsFutures"
    huge_down_url = "https://www.taifex.com.tw/cht/3/dailyHugeDealsFuturesDown"
    output_file = f"{output_dir}/huge.csv"
    payload={"queryDate": date, "doQuery": "1",
            "marketcode": marketcode, "MarketCode": marketcode, }
    
    with requests.Session() as s:
        s.headers.update(headers)
        s.post(huge_url, data=payload)
        with s.post(huge_down_url, data=payload) as resp:
            with open(output_file, 'wb') as f:
                f.write(resp.content)

def get_task_list(csvfile, date, marketcode):
    url = f"https://www.taifex.com.tw/cht/3/getFcmFutcontract.do?queryDate={date}&marketcode={marketcode}"
    future_list = []
    with requests.get(url,headers=headers) as resp:
        assert resp.status_code == 200
        jfile = resp.json()
        for i in jfile["commodityList"]:
            future_list.append(i["FDAILYR_KIND_ID"].strip())
    df = pd.read_csv(csvfile, encoding="big5", index_col=False)
    marketcode_dict = {0: "一般", 1: "盤後"}
    df = df[df.iloc[:, 9] != 0] # remove close volume is 0
    df = df[df.iloc[:, -2] == marketcode_dict[marketcode]] # filter the marketcode
    df = df[df.iloc[:, -1].isna()] # remove spread trans
    task_list = []
    for index, row in df.iterrows():
        fut_name = row.iloc[1].strip()
        if fut_name in future_list:
            task_list.append([fut_name, None, row.iloc[2].strip()])
        else:
            task_list.append(["STF", fut_name, row.iloc[2].strip()])
    return task_list
    
if __name__ == '__main__':
    if len(sys.argv[1]) == 0:
        sys.argv[1] = 0
    marketcode =  sys.argv[1]
    today = utils.get_today(post=int(marketcode))
    utils.create_dir(today)
    output_dir = f"{today}/bshtm_fut/{marketcode}"
    os.makedirs(output_dir,exist_ok=True)

    task_list = get_task_list(f"{today}/taifex/FUT_DAY_ALL.csv", today, int(marketcode))
    if len(task_list) != 0:
        download_data(task_list,today,marketcode)
    get_huge_bshtm(today,marketcode)
