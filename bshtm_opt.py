import requests
import asyncio
import aiohttp
import utils
import sys
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

normal_url = "https://www.taifex.com.tw/cht/3/dailyOptions"
download_url = "https://www.taifex.com.tw/cht/3/dailyOptionsDown"
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"}
    
def check_file_exist(task_list):
    not_exist_list = []
    for task in task_list:
        del_flag = False
        if task[0] == "STO":
            filename = f"{output_dir}/{task[1]}_{task[2]}_{task[3]}.csv"
        else:
            filename = f"{output_dir}/{task[0]}_{task[2]}_{task[3]}.csv"

        if not os.path.isfile(filename):
            not_exist_list.append(task)
        else:
            try:
                with open(filename,"r") as f:
                    first_line = f.readline()
                    if "HTML" in first_line:
                        not_exist_list.append(task)
                        del_flag = True
            except UnicodeDecodeError:
                continue

        if del_flag: os.remove(filename)

    return not_exist_list

def get_bshtm(task,date,marketcode):
    if task[0] == "STO":
        output_file = f"{output_dir}/{task[1]}_{task[2]}_{task[3]}.csv"
    else:
        output_file = f"{output_dir}/{task[0]}_{task[2]}_{task[3]}.csv"

    payload={"queryDate": date, "queryDateAh": date, 
            "commodityId": task[0], "commodityId2": task[1], "marketcode": marketcode, 
            "commodity_idt": task[0], "commodity_id2t": task[1], "MarketCode": marketcode, 
            "doQuery": "1", "settlemon": task[2], "pccode": task[3]}
    
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
    huge_url = "https://www.taifex.com.tw/cht/3/dailyHugeDealsOptions"
    huge_down_url = "https://www.taifex.com.tw/cht/3/dailyHugeDealsOptionsDown"
    output_file = f"{output_dir}/huge.csv"
    payload={"queryDate": date, "commodityId": None, "commodityId2": None,
            "marketcode": marketcode, "MarketCode": marketcode, 
        "doQuery": "1"}
    with requests.Session() as s:
        s.headers.update(headers)
        s.post(huge_url, data=payload)
        with s.post(huge_down_url, data=payload) as resp:
            with open(output_file, 'wb') as f:
                f.write(resp.content)

def get_task_list(csvfile, date, marketcode):
    url = f"https://www.taifex.com.tw/cht/3/getFcmOptcontract.do?queryDate={date}&marketcode={marketcode}"
    normal_list = []
    with requests.get(url,headers=headers) as resp:
        assert resp.status_code == 200
        jfile = resp.json()
        for i in jfile["commodityList"]:
            normal_list.append(i["FDAILYR_KIND_ID"].strip())
    df = pd.read_csv(csvfile, encoding="big5", index_col=False)
    marketcode_dict = {0: "一般", 1: "盤後"}
    callput_code_dict = {"賣權": "P", "買權": "C"}
    df = df[df.iloc[:, 9] != 0] # remove close volume is 0
    df = df[df.iloc[:, -3] == marketcode_dict[marketcode]] # filter the marketcode
    task_list = []
    for index, row in df.iterrows():
        fut_name = row.iloc[1].strip()
        if fut_name in normal_list:
            task_list.append([fut_name, None, row.iloc[2].strip(), callput_code_dict[row.iloc[4]]])
        else:
            task_list.append(["STO", fut_name, row.iloc[2].strip(), callput_code_dict[row.iloc[4]]])

    unique_list = []
    for item in task_list:
        if item not in unique_list:
            unique_list.append(item)

    return unique_list
    
if __name__ == '__main__':
    if len(sys.argv[1]) == 0:
        sys.argv[1] = 0
    marketcode =  sys.argv[1]
    today = utils.get_today(post=int(marketcode))
    utils.create_dir(today)
    output_dir = f"{today}/bshtm_opt/{marketcode}"
    os.makedirs(output_dir,exist_ok=True)

    task_list = get_task_list(f"{today}/taifex/OPT_DAY_ALL.csv", today, int(marketcode))
    if len(task_list) != 0:
        download_data(task_list,today,marketcode)
    get_huge_bshtm(today,marketcode)