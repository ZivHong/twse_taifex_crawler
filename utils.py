from datetime import datetime, timedelta
import os

def get_today(now=datetime.now(),post=False) -> str: 
    shift = 0
    # if today is weekend set to friday
    if now.weekday() >= 5:
        shift = 4 - now.weekday()
        # if post is true set to next monday
        if post:
            shift = 7 - now.weekday()
    else:
        # if today is before 3pm set to yesterday
        if now.hour < 15:
            shift = -1
            # if today is monday set to friday
            if now.weekday() == 0:
                shift = -3
            if post:
                shift = 0
    
    today = (now + timedelta(days=shift)).strftime("%Y%m%d")
    
    return today

def create_dir(dirname:str):
    os.makedirs(dirname,exist_ok=True)
    os.makedirs(dirname+"/bshtm",exist_ok=True)
    os.makedirs(dirname+"/bshtm_fut",exist_ok=True)
    os.makedirs(dirname+"/bshtm_opt",exist_ok=True)
    os.makedirs(dirname+"/taifex",exist_ok=True)
    os.makedirs(dirname+"/twse",exist_ok=True)