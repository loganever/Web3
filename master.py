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
        self.yellow = float(monitor_config['yellow'])       
        self.green = float(monitor_config['green'])              
        self.block_time_len = int(monitor_config['block_len'])    

    def get_config(self):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        sql = "SELECT * from rpc"
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        rpcs = []
        for i in result:
            rpcs.append(i[0])
        return {"rpc":rpcs}

    def recive_data(self,data,ip):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        for key in data['result'].keys():
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = "insert into detect(rpc_url,detect_time,result,elapse,block,status_code,headers,text,ip) \
                        values('%s','%s','%d','%f','%d','%d','%s','%s','%s')" % (key,dt,data['result'][key]['block']==data['newest'],data['result'][key]['elapse'],data['result'][key]['block'],data['result'][key]['status_code'],data['result'][key]['headers'].replace("'","\\'"),data['result'][key]['text'].replace("'","\\'"),ip)
            cursor.execute(sql)
        conn.commit()
        conn.close()

    # 获取所有节点最近num个时间区块的监测结果
    def get_data(self,num):
        conn = self.db_pool.connection()
        cursor = conn.cursor()
        # 获取数据
        sql = "SELECT rpc_url,timestampdiff(Hour,detect_time,now()) as h,result,count(*) as cnt FROM detect WHERE detect_time > (now() - INTERVAL 24 Hour) group by rpc_url, h, result order by rpc_url,h,result"
        cursor.execute(sql)
        result = cursor.fetchall()
        rpc_sql = "SELECT url,type,register,name from rpc"
        cursor.execute(rpc_sql)
        rpcresult = cursor.fetchall()
        conn.close()

        info = {}
        for i in rpcresult:
            info[i[0]] = {"type":i[1],"register":i[2],"name":i[3]}
        data = {}
        zero_cnt = 0
        for i in result:
            if i[0] not in info.keys():
                continue
            if i[0] not in data.keys():
                data[i[0]] = {}
                if info[i[0]]["type"]=='private':
                    data[i[0]] ["type"] = info[i[0]]["type"]
                    data[i[0]] ["register"] = info[i[0]]["register"]
                    data[i[0]] ["name"] = info[i[0]]["name"]
                else:
                    data[i[0]] ["type"] = info[i[0]]["type"]
                    data[i[0]] ["name"] = info[i[0]]["name"]
                data[i[0]]["color"] = []
            if len(data[i[0]]["color"])>=num:
                continue
            if i[2]==0:
                if zero_cnt!=-1:
                    data[i[0]]["color"].append('red')
                else:
                    zero_cnt = i[3]
            else:
                if zero_cnt/(zero_cnt+i[3])<=self.green:
                    data[i[0]]["color"].append('green')
                elif zero_cnt/(zero_cnt+i[3])<=self.yellow:
                    data[i[0]]["color"].append('yellow')
                else:
                    data[i[0]]["color"].append('red')
                zero_cnt = -1
        for i in data:
            if len(data[i]["color"])<num:          # 监测数据不足则填充null
                data[i]["color"].extend(['null']*(num-len(data[i]["color"])))
        return data

    # 增加rpc节点
    # def add_rpc(self,rpcs):
    #     self.test_conn()
    #     cursor = self.db.cursor()
    #     for url in rpcs:
    #         sql = "insert into config(rpc_url) values('%s')" % (url)
    #         cursor.execute(sql)
    #     cursor.close()
    #     self.db.commit()



 
app = Flask(__name__)
 
@app.route("/config",methods=["GET"])
def config(): 
    return master.get_config()
 
@app.route("/recive",methods=["POST"]) 
def recive():
    data = dict(request.json)
    ip = request.remote_addr
    master.recive_data({"result":data['result'], "newest":data['newest']},ip)
    return "ok"

@lru_cache()
def real_get_data(_ts):
    logging.info("query database data")
    try:
        num = request.args.get('num')
        color = master.get_data(int(num))
        data =[]
        for key in color:
            if color[key]['type']=='private':
                data.append({"url":color[key]['name'],"type":color[key]['type'],"register":color[key]['register'],"name":color[key]['name'],"detect":color[key]['color']})
            else:
                data.append({"url":key,"type":color[key]['type'],"name":color[key]['name'],"detect":color[key]['color']})
        return {"status":"ok","data":data}
    except Exception as err:
        logging.info(traceback.format_exc())
        return {"status":"fail"}

@app.route("/get_data",methods=["GET"]) 
def get_data():
    return real_get_data(int(time.time())/600)

# @app.route("/add_rpc",methods=["POST"])
# def add_rpc():
#     master.add_rpc(json.loads(request.data.decode())['rpcs'])
#     return "ok"
 
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
