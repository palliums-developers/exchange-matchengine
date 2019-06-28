#!/user/bin/python3

import time
import socket
import asyncio
import functools
import traceback

import pymysql
import requests
import json
import logging

import queue

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask

import sys

def dot(a):
    sys.stdout.write(a)
    sys.stdout.flush()

########################################################################################################################

gcnt = 0
executor = ThreadPoolExecutor(max_workers=20)

def get_html(i):
    print('task %d...' % i)
    #r  = requests.get('http://127.0.0.1:5000/')
    r  = requests.get('http://127.0.0.1:5000/user/jack')
    print(r.text)
    r  = requests.post('http://127.0.0.1:5000/user/join')
    print(r.text)
    r  = requests.post('http://127.0.0.1:5000/hello/michael')
    print(r.text)
    r  = requests.get('http://127.0.0.1:5000/projects/?name=jack')
    print(r.text)
    data = {'age': 8 }
    r  = requests.post('http://127.0.0.1:5000/projects/', data = data)
    print(r.text)

    return i

# urls = list(range(100))
# all_task = [executor.submit(get_html, (url)) for url in urls]

# for future in as_completed(all_task):
#     data = future.result()
#     print("in main: get page {}s success".format(data))

if __name__ == '__main__':
    get_html(0)
    pass

########################################################################################################################
