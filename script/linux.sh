#!/usr/bin/bash
source /home/user/miniforge3/etc/profile.d/conda.sh
cd /home/user/stock
conda activate stock
marketcode=$1
python taifex.py $marketcode
python bshtm_fut.py $marketcode
python bshtm_opt.py $marketcode
if [ "$marketcode" = "0" ] 
    then
    python bshtm_stock.py
fi