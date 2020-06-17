import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse

import pandas as pd
import requests

from config import DINGDING


def send_dingding_msg(msg):
    timestamp = str(round(time.time() * 1000))
    secret = DINGDING['secret']
    access_token = DINGDING['access_token']
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    url = f'https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}'
    headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
    req = {"msgtype": "text", "text": {"content": msg}}
    req_json = json.dumps(req)
    requests.post(url=url, data=req_json, headers=headers)


def update_data(path, df):
    if os.path.exists(path):
        df_old = pd.read_hdf(path)
        intersection = df_old.index.intersection(df.index)
        if not intersection.empty:
            df_old.drop(df_old.index.intersection(df.index), inplace=True)
        df_ret = pd.concat((df_old, df))
    else:
        df_ret = df
    df_ret = df_ret.sort_index()
    df_ret.to_hdf(path, 'data')
