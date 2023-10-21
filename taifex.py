import aiohttp
import asyncio
import requests
import os
import shutil
import utils
import sys

def get_fut_opt_day_all(today:str, extract_dir:str):
    today = f"{today[:4]}/{today[4:6]}/{today[6:8]}"
    payload = {"down_type": "1", "commodity_id": "all", "commodity_id2": None, 
               "queryStartDate": today, "queryEndDate": today}

    with requests.post("https://www.taifex.com.tw/cht/3/optDataDown", data=payload) as resp:
        with open(f"{extract_dir}/OPT_DAY_ALL.csv", "wb") as f:
            f.write(resp.content)
    
    with requests.post("https://www.taifex.com.tw/cht/3/futDataDown", data=payload) as resp:
        with open(f"{extract_dir}/FUT_DAY_ALL.csv", "wb") as f:
            f.write(resp.content)


async def get_zip_taifex_async(url, extract_dir):
    name = url.split("/")[-2]
    
    temp_zip = f"{name}.zip"
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        with open(temp_zip, "wb") as f:
            f.write(await response.content.read())
    
    shutil.unpack_archive(temp_zip, extract_dir)
    os.remove(temp_zip)

async def main(url_list,extract_dir): 
    tasks = []
    for url in url_list:
        tasks.append(asyncio.create_task(get_zip_taifex_async(url,extract_dir)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    if len(sys.argv[1]) == 0:
        sys.argv[1] = 0
    marketcode =  int(sys.argv[1])

    today = utils.get_today(post=marketcode)
    year = today[:4]
    month = today[4:6]
    day = today[6:8]
    utils.create_dir(today)
    taifex_dir = f"{today}/taifex"
    download_url =[
        f"https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_{year}_{month}_{day}.zip",
        f"https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV_B/Daily_{year}_{month}_{day}_B.zip",
        f"https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV_C/Daily_{year}_{month}_{day}_C.zip",
        f"https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_{year}_{month}_{day}.zip"
    ]
    
    if marketcode == 0:
        asyncio.run(main(download_url,taifex_dir))

    get_fut_opt_day_all(today, taifex_dir)