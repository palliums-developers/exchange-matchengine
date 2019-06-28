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
from flask import request
from flask import url_for
from flask import render_template

import redis

import sys

def dot(a):
    sys.stdout.write(a)
    sys.stdout.flush()

########################################################################################################################

r = redis.Redis(host='127.0.0.1', port=6379)
r.set('name', 'jack')
print(r.get('name'))

########################################################################################################################

# app = Flask(__name__)

# gcnt = 0
# lock = threading.Lock()

# def gcnt_incr():
#     global gcnt
#     with lock: gcnt += 1
    
# @app.route('/')
# def hello():
#     gcnt_incr()
#     return 'hello jack! %d' % gcnt 

# @app.route('/userid/<int:userid>')
# def userid(userid):
#     return 'userid: %d' % userid

# @app.route('/user/<username>', methods = ['GET', 'POST'])
# def show_user_profile(username):
#     if request.method == 'GET':
#         return '%s User %s' % ('get', username)
#     if request.method == 'POST':
#         return '%s User %s' % ('post', username)
#     return 'not support method'

# @app.route('/hello/<name>', methods = ['GET','POST'])
# def helloname(name=None):
#     return render_template('hello.html', name=name)

# @app.route('/projects/', methods = ['GET', 'POST'])
# def projects():
#     if request.method == 'GET':
#         a = request.args.get('name', 'unknown')
#         return 'name: %s' % a
#     if request.method == 'POST':
#         b = request.form.get('age', '0')
#         return 'age: %d' % int(b)

# @app.route('/ajax/demo_test.txt', methods = ['GET', 'POST'])
# def demo_test():
#     with open('ajax/demo_test.txt') as f:
#         text = f.read()
#         return text

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', debug=True)


########################################################################################################################

# import gevent
# import dnslib
# from gevent import socket
# from gevent import event
# from gevent import monkey

# monkey.patch_all()

# rev=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
# rev.bind(('',53))
# ip=[]
# cur=0
 
# def preload():
#     for i in open('ip'):
#         ip.append(i)
#     print "load "+str(len(ip))+"ip"
 
# def send_request(data):
#     global cur
#     ret=rev.sendto(data,(ip[cur],53))
#     cur=(cur+1)%len(ip)
 
# class Cache:
#     def __init__(self):
#         self.c={}
#     def get(self,key):
#         return self.c.get(key,None)
#     def set(self,key,value):
#         self.c[key]=value
#     def remove(self,key):
#         self.c.pop(key,None)
 
# cache=Cache()
 
# def handle_request(s,data,addr):
#     req=dnslib.DNSRecord.parse(data)
#     qname=str(req.q.qname)
#     qid=req.header.id
#     ret=cache.get(qname)
#     if ret:
#         ret=dnslib.DNSRecord.parse(ret)
#         ret.header.id=qid;
#         s.sendto(ret.pack(),addr)
#     else:
#         e=event.Event()
#         cache.set(qname+"e",e)
#         send_request(data)
#         e.wait(60)
#         tmp=cache.get(qname)
#         if tmp:
#             tmp=dnslib.DNSRecord.parse(tmp)
#             tmp.header.id=qid;
#             s.sendto(tmp.pack(),addr)
 
# def handle_response(data):
#     req=dnslib.DNSRecord.parse(data)
#     qname=str(req.q.qname)
#     print qname
#     cache.set(qname,data)
#     e=cache.get(qname+"e")
#     cache.remove(qname+"e")
#     if e:
#         e.set()
#         e.clear()
 
# def handler(s,data,addr):
#     req=dnslib.DNSRecord.parse(data)
#     if req.header.qr:
#         handle_response(data)
#     else:handle_request(s,data,addr)
 
# def main():
#     preload()
#     while True:
#         data,addr=rev.recvfrom(8192)
#         gevent.spawn(handler,rev,data,addr)
 
# if __name__ == '__main__':
#     main()
    
########################################################################################################################

# TESTRPCPORT = "18332"
# TESTRPCHOST = "125.39.5.57"
# TESTRPCUSER = "palliums"
# TESTRPCPASSWORD = "qazpl,"

# dataobj = {"jsonrpc": "1.0", "id":"curltest", "method": "getblockcount", "params": [] }
# headers = {'content-type': 'text/plain'}

# try:
#     r = requests.post("http://palliums:qazpl,@125.39.5.57:18332", data = json.dumps(dataobj), headers = headers)
#     print(r.text)
#     rsp = json.loads(r.text)
#     print(rsp)
    
# except Exception as err:
#     print(err)

class BitcoinRpc:
    
    def __init__(self, ip, port, user, passwd):
        self.url = "http://%s:%s@%s:%d" % (user, passwd, ip, port)
        self.headers = {'content-type': 'text/plain'}
        
    def send(self, method, params, id = "test"):
        data = {"jsonrpc": "1.0", "id":id, "method": method, "params": params }
        try:
            r = requests.post(self.url, data = json.dumps(data), headers = self.headers)
            #print('req:', r)
            rsp = json.loads(r.text)
            with open('rsp.json', 'w') as f:
                f.write(r.text)
            return rsp
        except Exception as err:
            #print(err)
            logging.exception(err)

    def getblockcount(self, id = "test"):
        rsp = self.send("getblockcount", [], id)
        return rsp['result']

    def getblockhash(self, height, id = "test"):
        rsp = self.send("getblockhash", [height], id)
        return rsp['result']

    def getblock(self, hash, id = "test"):
        rsp = self.send("getblock", [hash, 2], id)
        return rsp['result']
    
rpc = BitcoinRpc("125.39.5.57", 18332, "palliums", "qazpl,")


#count = rpc.getblockcount()
# count = 1544008
# hash = rpc.getblockhash(count)
# print(count)
# block = rpc.getblock(hash)
# print("tx cnt: ", len(block['tx']))
# print(json.dumps(block['tx'][1], indent=2))

def getblock(height):
    hash = rpc.getblockhash(height)
    return rpc.getblock(hash)

# height = rpc.getblockcount()
# executor = ThreadPoolExecutor(max_workers=80)
# count = 200

# tasks = [executor.submit(getblock, (i)) for i in range(height, height-count, -1)]

# cnt = 0

# start_time = time.time()

# for future in as_completed(tasks):
#     print(future.result())
#     cnt += 1
#     print("cnt: ", cnt)

# end_time = time.time()

# print('time cost', end_time-start_time, 's')

# qblocks = queue.Queue() 

# def getblockfunc(i):
#     last_height = rpc.getblockcount()-3
#     while True:
#         height = rpc.getblockcount()
#         if last_height < height:
#             qblocks.put(getblock(last_height))
#             last_height += 1
#         time.sleep(1)
#         dot('.')
    
# tgetblock = threading.Thread(target = getblockfunc, args = str(0))
# tgetblock.start()

# while True:
#     block = qblocks.get()
#     print(block)
#     qblocks.task_done();
#     dot('*')
    
# tgetblock.join()

########################################################################################################################

def db_new_project():
    
    db = pymysql.connect("47.106.208.207","root","1234qwer","test" )

    cursor = db.cursor()

    id = 79
    no = "FM00%d" % id
    name = "money00%d" % id
    desc = "makemoney00%d" % id
    now1 = int(time.time());
    now2 = now1 + 9*24*3600;
    crowdfunding_address = "mousWBSN7Rsqi8qpmZp7C6VmRkBGPD5bFF"
    projecter_publickey = "031d674ea43b0eee2d1307c8244bcb46823f477b8270baa8f17af14bde64a1f5a5"
    financial_controller_publickey = "037982270eca456b4d978df2c4fb03f681e320fc6682ea042dcff15766b8360e42"
    crowdfunding_payment_txid = "crowdfunding_payment_txid"

    sql = """
    INSERT INTO `test`.`financial_management_projects` (`id`, `no`, `name`, `annualized_return_rate`, `investment_period`, `min_invest_amount`, `max_invest_amount`, `total_crowdfunding_amount`, `booking_starting_time`, `crowdfunding_ending_time`, `status`, `received_crowdfunding`, `interest_type`, `borrower_info`, `crowdfunding_address`, `product_description`, `projecter_publickey`, `financial_controller_publickey`, `crowdfunding_payment_txid`) VALUES (%d, '%s', '%s', '0.083', '30', '0.001', '0.1', '3', '%d', '%d', '0', '0', '0', '0', '%s', '%s', '%s', '%s', '%s')
    """ % (id, no, name, now1, now2, crowdfunding_address, desc, projecter_publickey, financial_controller_publickey, crowdfunding_payment_txid)
    
    print(sql)

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as err:
        print("query failed")
        print(err)
        #traceback.print_exc()
        db.rollback()

    db.close()

#db_new_project()

########################################################################################################################

# print("hello world")

# HOST = '127.0.0.1'
# PORT = 60001

# with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#     s.connect((HOST, PORT))
#     s.sendall(b'hello world@#$')
#     data = s.recv(1204)
    
# print('received', str(data))


# now = lambda : time.time()

# async def dosomework(loop, x):
#     print("waiting:", x)
#     await asyncio.sleep(x)
#     print("after sleep:", x)
#     #asyncio.ensure_future(dosomework(x+1))
#     return "Done after {}s".format(x)

# start = now()


# loop = asyncio.get_event_loop()

# coroutine1 = dosomework(loop, 3)
# coroutine2 = dosomework(loop, 6)
#coroutine3 = dosomework(loop, 9)

# tasks = [
#     asyncio.ensure_future(coroutine1),
#     asyncio.ensure_future(coroutine2),
#     asyncio.ensure_future(coroutine3)
# ]

#asyncio.ensure_future(coroutine1)
#asyncio.ensure_future(coroutine2)

# futus = asyncio.gather(coroutine1, coroutine2)

# def callback(loop, futu):
#     loop.stop()
    
# futus.add_done_callback(functools.partial(callback, loop))

# loop.run_forever()

#loop.run_until_complete(asyncio.wait(tasks))

# for task in tasks:
#     print("task ret:", task.result())

# print("Time:", now()-start)

########################################################################################################################
