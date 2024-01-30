import requests
from bs4 import BeautifulSoup
import onnxruntime
import numpy as np
import cv2
import os
import logging
import utils
import twse
import time
import pandas as pd
import shutil
import csv
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def get_stock():
    ok_list = []
    stock_list = []

    for file in os.listdir(f'{today}/bshtm'):
        filename_without_extension = os.path.splitext(file)[0]
        ok_list.append(filename_without_extension)

    stock_csv = f'{today}/twse/STOCK_DAY_ALL.csv'

    if not os.path.isfile(stock_csv):
        twse.get_stock_day_all(today)

    df = pd.read_csv(stock_csv)
    filtered_df = df[df.iloc[:, 2] != 0]
    stock_list = filtered_df.iloc[:, 0].tolist()
    stock_list.append("excd")
    stock_list = [item for item in stock_list if item not in ok_list]
    
    return stock_list

def gen_payload(url,excd=False):
    payload = {}
    with s.get(url) as resp:
        soup = BeautifulSoup(resp.text, 'html.parser')
        img_url = soup.select("#Panel_bshtm img")
        if len(img_url) == 0:
            raise Exception("Can't retrieve captcha url")
        payload["img_url"] = img_url[0].get('src')
        payload["__EVENTTARGET"] = ""
        payload["__EVENTARGUMENT"] = ""
        payload["__LASTFOCUS"] = ""
        payload["__VIEWSTATE"] = soup.find(id="__VIEWSTATE").get('value')
        payload["__VIEWSTATEGENERATOR"] = soup.find(id="__VIEWSTATEGENERATOR").get('value')
        payload["__EVENTVALIDATION"] = soup.find(id="__EVENTVALIDATION").get('value')
        if excd:
            payload["RadioButton_Excd"] = "RadioButton_Excd"
        else:
            payload["RadioButton_Normal"] = "RadioButton_Normal"
        payload["btnOK"] = "查詢"

    return payload

def ocr(captcha_url):
    with s.get(captcha_url) as r:
        if len(r.content) < 1500:
            raise Exception("Captcha not found")
        file_bytes = np.frombuffer(r.content, dtype=np.uint8)
        img = cv2.imdecode(file_bytes, cv2.IMREAD_GRAYSCALE)
        blur1 = cv2.fastNlMeansDenoising(img, None, 50, 7, 21)
        _,captcha_img = cv2.threshold(blur1, 210, 255, cv2.THRESH_BINARY) 
        captcha_img = np.expand_dims(captcha_img, axis=0) / 255.
        captcha_img = (captcha_img - 0.456) / 0.224

        ort_inputs = {'input1': np.array([captcha_img]).astype(np.float32)}
        ort_outs = ort_session.run(None, ort_inputs)
        last_item = 0
        charset = ["", "2", "3", "4", "6", "7", "8", "9", "A", "C", "D", "E", "F", "G", "H", "J", "K", "L", "N", "P", "Q", "R", "T", "U", "V", "X", "Y", "Z"]
        result = ""
        for item in ort_outs[0][0]:
            if item == last_item:
                continue
            else:
                last_item = item
            if item != 0:
                result += charset[item]
        return result

if __name__ == '__main__':
    model_path = "bshtm_captcha_gray.onnx"
    ort_session = onnxruntime.InferenceSession(model_path, providers=['CPUExecutionProvider'])

    today = utils.get_today()
    utils.create_dir(today)
    
    logging.basicConfig(filename=f'{today}/bshtm.log', level=logging.INFO,format='%(asctime)s | %(levelname)8s | %(message)s')
    
    logging.info("Crawler start")

    ip_list = ['117.56.218.176', '122.147.34.176', '163.29.17.176', '210.65.84.176', '220.229.103.176', '61.57.47.176']
    ip_pivot = 0
    proxy_pivot = 0
    count = 1
    stock_list = []
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"}
    s = requests.session()
    s.headers = headers 
    s.verify = False
    try:
        stock_list = get_stock()
    except Exception as e:
        logging.error(f'Get stock_day_all Fail\t{e}')
        logging.error(f'Crawler Fail')
        os._exit(0)

    if len(stock_list)==0:
        logging.info(f'All stocks had downloaded')
    else:
        for stock in stock_list:
            base_ip = ip_list[ip_pivot]
            ip_pivot = (ip_pivot+1)%len(ip_list)
            
            base_url = f"https://{base_ip}/bshtm"
            bsMenu_url = f"https://{base_ip}/bshtm/bsMenu.aspx"
            # if count > proxy_count[proxy_pivot]:
            #     count = 1
            #     proxy_pivot = (proxy_pivot+1)%len(proxy_list)
            # s.proxies = {"https": proxy_list[proxy_pivot]}
            count+=1
            fail_flag = 0
            while 1:
                if fail_flag:
                    time.sleep(1)
                try:
                    payload = gen_payload(bsMenu_url,stock == "excd")
                    img_url = payload.pop("img_url")
                    captcha_text = ocr(f"{base_url}/{img_url}")

                    if len(captcha_text)!=5: 
                        raise Exception("Captcha length error")
                    else:
                        payload["CaptchaControl1"] = captcha_text
                        payload["TextBox_Stkno"] = stock
                        with s.post(bsMenu_url, data = payload) as resp:
                            soup = BeautifulSoup(resp.text, 'html.parser')
                            error_text  = soup.find(id="Label_ErrorMsg").text
                            
                            if len(error_text) == 5: #驗證碼錯誤
                                raise Exception("Captcha recognize error") 
                            elif len(error_text) == 4: #查無資料
                                logging.info(f'{stock} maybe is big deal')
                                break 
                            elif soup.find(id="HyperLink_DownloadCSV") != None:
                                if stock == "excd":
                                    download_url = f"{base_url}/bsExcdContent.aspx"
                                else:
                                    download_url = f"{base_url}/bsContent.aspx"
                                download_page = s.get(download_url)
                                with open(f'{today}/bshtm/{stock}.csv', 'wb') as f:
                                    f.write(download_page.content)
                                with open(f'{today}/bshtm/{stock}.csv', encoding="big5") as csvfile:
                                    csv_reader = csv.reader(csvfile)
                                    for row in csv_reader:
                                        if len(row[0]) > 14:
                                            raise Exception("content is broken")
                                logging.info(f'Get {stock} OK')
                                break
                            else: 
                                raise Exception("Unknown error")

                except Exception as e:
                    logging.error(f'{stock} {e}')
                    fail_flag = 1
                    continue
        
    logging.info(f'Crawler Finish')
    shutil.make_archive(f"archived/{today}", 'zip', today)
    logging.info(f'{today} has archived')
