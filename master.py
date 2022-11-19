# -*- coding: UTF-8 -*- 
import pymysql
import logging
import json
import datetime
import time
from pymysql.cursors import DictCursor
from flask import Flask, request
import configparser
import traceback
from optparse import OptionParser
from DBUtils.PooledDB import PooledDB, SharedDBConnection
from functools import lru_cache
import threading

# 日志设置
logging.basicConfig(filename='master_log.txt', level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


class Master:

    def __init__(self):
        self.db_pool = PooledDB(creator=pymysql,
            maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=3,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=1,  # 链接池中最多共享的链接数量，0和None表示全部共享
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            ping=0,
            # ping MySQL服务端，检查是否服务可用。
            # 如：0 = None = never,
            # 1 = default = whenever it is requested,
            # 2 = when a cursor is created,
            # 4 = when a query is executed,
            # 7 = always
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset']
        )            
        self.block_time_len = int(monitor_config['block_len'])   
        self.rpc_version = 0
        self.code_version = 0
        self.last_clean = time.time()
        self.clean_time = int(monitor_config['clean_time']) 

    def update_config(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "select rpc_version,code_version from config"
        cursor.execute(sql)
        result = cursor.fetchone()
        conn.close()
        self.rpc_version = int(result[0])
        self.code_version = int(result[1])
        return {"rpc_version":self.rpc_version,"code_version":self.code_version}

    def clean(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "delete from detect where detect_time < curdate()  - INTERVAL 30 hour"
        cursor.execute(sql)
        conn.commit()
        conn.close()
        self.last_clean = time.time()

    def get_config(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "SELECT url,chain from rpc"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        rpcs = []
        chains = []
        for i in result:
            rpcs.append(i[0])
            chains.append(i[1])
        return {"rpc":rpcs,"chain":chains, "rpc_version": self.rpc_version, "code_version":self.code_version}

    def get_private(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "SELECT url,register,name,location,free,price,support,main_use,chain from rpc where type='private'"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        data = {}
        for i in result:
            if i[8] not in data.keys():
                data[i[8]] = []
            data[i[8]].append({"url":i[0],"register":i[1],"name":i[2],"location":i[3],"free":i[4],"price":i[5],"support":i[6],"main_use":i[7]})
        return data

    def recive_data(self,data,ip):
        # 清理表
        if time.time()-self.last_clean > self.clean_time:
            thread = threading.Thread(target=self.clean)
            thread.start()  
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        for key in data['result'].keys():
            chain = ''
            if 'chain' not in data['result'][key]:
                chain = 'Ethereum Mainnet'
            else:
                chain = data['result'][key]['chain']
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = "insert into detect(rpc_url,detect_time,result,elapse,block,status_code,headers,text,ip,chain) \
                        values('%s','%s','%d','%f','%d','%d','%s','%s','%s','%s')" % (key,dt,data['result'][key]['block']==data['newest'],data['result'][key]['elapse'],data['result'][key]['block'],data['result'][key]['status_code'],data['result'][key]['headers'].replace("'","\\'"),data['result'][key]['text'].replace("'","\\'"),ip,chain)
            cursor.execute(sql)
        conn.commit()
        conn.close()

    # 获取所有节点最近num小时的监测结果
    @lru_cache()
    def get_data(self,num,_ts):
        logging.info("query database data")
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        # 获取数据
        sql = "SELECT chain,rpc_url,timestampdiff(Hour,detect_time,now()) as h,result,count(*) as cnt FROM detect WHERE detect_time > (now() - INTERVAL 24 Hour) group by chain,rpc_url, h, result order by chain,rpc_url,h,result"
        cursor.execute(sql)
        result = cursor.fetchall()
        rpc_sql = "SELECT url,type,register,name,location from rpc"
        cursor.execute(rpc_sql)
        rpcresult = cursor.fetchall()
        conn.close()

        info = {}
        for i in rpcresult:
            info[i[0]] = {"type":i[1],"register":i[2],"name":i[3],"location":i[4]}
        data = {}
        zero_cnt = -1
        last_url = ''
        last_chain = ''
        for i in result:
            if i[1] not in info.keys():
                continue
            if i[0] not in data.keys():
                data[i[0]] = {}
            if i[1] not in data[i[0]].keys():
                data[i[0]][i[1]] = {}
                if info[i[1]]["type"]=='private':
                    data[i[0]][i[1]]["register"] = info[i[1]]["register"]
                data[i[0]][i[1]]["type"] = info[i[1]]["type"]
                data[i[0]][i[1]]["name"] = info[i[1]]["name"]
                data[i[0]][i[1]]["location"] = info[i[1]]["location"]
                data[i[0]][i[1]]["url"] = i[1]
                data[i[0]][i[1]]["detect"] = []
            if len(data[i[0]][i[1]]["detect"])>=num:
                continue
            # 防止全0漏一个
            if last_url!=i[1] and zero_cnt!=-1:
                data[last_chain][last_url]["detect"].append(1.0)
            last_url = i[1]
            last_chain = i[0]
            # 计算失败率
            if i[3]==0:
                if zero_cnt!=-1:
                    data[i[0]][i[1]]["detect"].append(1.0)
                zero_cnt = i[4]
            else:
                if zero_cnt==-1:
                    zero_cnt = 0
                data[i[0]][i[1]]["detect"].append(round(zero_cnt/(zero_cnt+i[4]),4))
                zero_cnt = -1
                
        if zero_cnt!=-1:
            data[last_chain][last_url]["detect"].append(1.0)

        processed_data = {}
        for i in data.keys():
            processed_data[i] = []
            for j in data[i].keys():
                if len(data[i][j]["detect"])<num:
                    data[i][j]["detect"].extend([None]*(num-len(data[i][j]["detect"])))
                processed_data[i].append(data[i][j])
        return processed_data

 
app = Flask(__name__)

@app.route("/update",methods=["GET"])
def update(): 
    return master.update_config()
 
@app.route("/config",methods=["GET"])
def config(): 
    return master.get_config()
 
@app.route("/recive",methods=["POST"]) 
def recive():
    data = dict(request.json)
    ip = request.remote_addr
    master.recive_data({"result":data['result'], "newest":data['newest']},ip)
    return {"status":"ok","rpc_version":master.rpc_version,"code_version":master.code_version}

@app.route("/get_data",methods=["GET"]) 
def get_data():
    try:
        num = request.args.get('num')
        data = master.get_data(int(num),int(int(time.time())/600))
        return {"status":"ok","data":data}
    except Exception as err:
        logging.info(traceback.format_exc())
        return {"status":"fail"}

@app.route("/get_private",methods=["GET"]) 
def get_private():
    try:
        return {"status":"ok","data":master.get_private()}
    except Exception as err:
        logging.info(traceback.format_exc())
        return {"status":"fail"}

 
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-c", "--config", help="配置文件路径")
    (options, args) = parser.parse_args()
    file = options.config

    #读取配置文件
    con = configparser.ConfigParser()
    con.read(file, encoding='utf-8')

    db_config = dict(con.items('db'))
    monitor_config = dict(con.items('monitor'))
    server_config = dict(con.items('server'))

    master = Master()
    app.run(host='0.0.0.0', port=int(server_config['port'])) #运行app
