#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/8 9:28
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import random
import string

import requests


def get_joom_token():
    req_url = "https://www.joom.com/tokens/init"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-length": "0",
        "origin": "https://www.joom.com",
        "referer": "https://www.actneed.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
        "x-version": "0.1.0"
    }
    try_times = 3
    while try_times > 0:
        try:
            session = requests.session()
            res = session.post(req_url, headers=headers)
            result = res.json()
            # redis_conn.set("joom_token", "Bearer %s" % result["accessToken"])
            return "Bearer %s" % result["accessToken"]
        except requests.ConnectTimeout:
            try_times -= 1
        except Exception, e:
            print e
            return False


def random_key(length=48):
    return ''.join([random.choice(string.letters) for i in xrange(length)])
