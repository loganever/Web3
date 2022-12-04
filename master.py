# -*- coding: UTF-8 -*- 
import pymysql
import logging
import json
import datetime
import time
from pymysql.cursors import DictCursor
from flask import Flask, request
import configparser
from optparse import OptionParser
from DBUtils.PooledDB import PooledDB, SharedDBConnection
from functools import lru_cache
import threading
from gevent import pywsgi

logging.basicConfig(filename='master_log.txt', level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


class Master:

    def __init__(self):
        self.db_pool = PooledDB(creator=pymysql,
            maxconnections=0,
            mincached=3,
            maxcached=0,
            maxshared=3,
            blocking=True,
            maxusage=None,
            ping=0,
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset']
        )            
        self.block_time_len = int(monitor_config['block_len'])   
        self.rpc_version = 1
        self.code_version = 0
        self.last_clean = time.time()
        self.clean_time = int(monitor_config['clean_time']) 
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "select chain,block_diff from chain"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        self.chain_block_diff = {}
        for i in result:
            self.chain_block_diff[i[0]] = int(i[1])
        logging.info(self.chain_block_diff)

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
        try:
            conn = self.db_pool.connection()
            cursor = conn.cursor()
            sql = "delete from detect where detect_time < curdate()  - INTERVAL 25 hour"
            cursor.execute(sql)
            sql = "delete from Error where detect_time < curdate()  - INTERVAL 25 hour"
            cursor.execute(sql)
            conn.commit()
            conn.close()
            self.last_clean = time.time()
            logging.info("clean ok")
        except Exception as e:
            logging.error(e)
            logging.info("clean fail")

    def get_config(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "SELECT rpc.url,rpc.chain,chain.payload from rpc,chain where rpc.chain=chain.chain"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        rpcs = []
        chains = []
        payloads = []
        for i in result:
            rpcs.append(i[0])
            chains.append(i[1])
            payloads.append(i[2])
        return {"rpc":rpcs,"chain":chains,"payload":payloads,"rpc_version": self.rpc_version, "code_version":self.code_version}

    def get_rpc_info(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "SELECT DISTINCT register,name,location,free,price,support,main_use,chain,privacy from rpc where free is not null"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        data = {}
        for i in result:
            if i[7] not in data.keys():
                data[i[7]] = []
            data[i[7]].append({"register":i[0],"name":i[1],"location":i[2],"free":i[3],"price":i[4],"support":i[5],"main_use":i[6],"privacy":i[8]})
        return data

    def recive_data(self,data,ip):
        # 清理表
        if time.time()-self.last_clean > self.clean_time:
            thread = threading.Thread(target=self.clean)
            thread.start()  
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "insert into detect(rpc_url,detect_time,result,ip,chain) values"
        error_sql = "insert into Error(rpc_url,detect_time,status_code,headers,text,ip,chain) values"
        error_flag = False
        for key in data['result'].keys():
            chain = data['result'][key]['chain']
            text = data['result'][key]['text'].replace("'","\\'")
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            result = abs(data['result'][key]['block']-data['newest'])<=self.chain_block_diff[chain]
            sql+="('%s','%s','%d','%s','%s')," % (key,dt,result,ip,chain)
            if result==False:
                error_flag = True
                error_sql+="('%s','%s','%d','%s','%s','%s','%s')," % (key,dt,data['result'][key]['status_code'],data['result'][key]['headers'].replace("'","\\'"),text[:min(len(text),2000)],ip,chain)
        cursor.execute(sql[:-1])
        if error_flag:
            cursor.execute(error_sql[:-1])
        conn.commit()
        conn.close()

    @lru_cache()
    def get_data(self,num,_ts):
        logging.info("query database data")
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        # 获取数据
        sql = "SELECT chain,rpc_url,timestampdiff(Hour,detect_time,now()) as h,result,count(rpc_url) as cnt FROM detect WHERE detect_time > (now() - INTERVAL 24 Hour) group by chain,rpc_url, h, result order by chain,rpc_url,h,result"
        cursor.execute(sql)
        result = cursor.fetchall()
        rpc_sql = "SELECT url,type,register,name,location,privacy from rpc"
        cursor.execute(rpc_sql)
        rpcresult = cursor.fetchall()
        conn.close()

        info = {}
        for i in rpcresult:
            info[i[0]] = {"type":i[1],"register":i[2],"name":i[3],"location":i[4],"privacy":i[5]}
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
                data[i[0]][i[1]]["privacy"] = info[i[1]]["privacy"]
                data[i[0]][i[1]]["url"] = i[1]
                data[i[0]][i[1]]["detect"] = []
            if len(data[i[0]][i[1]]["detect"])>=num:
                continue

            if last_url!=i[1] and zero_cnt!=-1:
                data[last_chain][last_url]["detect"].append(1.0)
                zero_cnt = -1
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
    logging.info("get config")
    return master.get_config()
 
@app.route("/recive",methods=["POST"])
def recive():
    data = dict(request.json)
    ip = request.remote_addr
    if 'node_name' in data.keys():
        logging.info(data['node_name'])
    else:
        logging.info(ip)
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
        return {"status":"ok","data":master.get_rpc_info()}
    except Exception as err:
        logging.info(err)
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
    server = pywsgi.WSGIServer(('0.0.0.0', int(server_config['port'])), app)
    server.serve_forever()
    # app.run(host='0.0.0.0', port=int(server_config['port'])) #运行app
