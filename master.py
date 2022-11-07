import pymysql
import logging
import json
import datetime
import time
from pymysql.cursors import DictCursor
from flask import Flask, request
import configparser
from optparse import OptionParser

# 日志设置
logging.basicConfig(filename='master_log.txt', level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


class Master:

    def __init__(self):
        self.db = pymysql.connect(
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

    def __del__(self):
        self.db.close()

    def test_conn(self):
     try:       # 检查数据库连接是否断开
         self.db.ping()
     except:    # 断开则重连
         self.db = pymysql.connect(
            host=db_config['host'], 
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset']
            )

    def get_config(self):
        self.test_conn()
        cursor = self.db.cursor(DictCursor)
        sql = "SELECT * from rpc"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        rpcs = []
        for i in result:
            rpcs.append(i['url'])
        return {"rpc":rpcs}

    def recive_data(self,data,ip):
        self.test_conn()
        cursor = self.db.cursor()
        for key in data['result'].keys():
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = "insert into detect(rpc_url,detect_time,result,elapse,block,status_code,headers,text,ip) \
                        values('%s','%s','%d','%f','%d','%d','%s','%s','%s')" % (key,dt,data['result'][key]['block']==data['newest'],data['result'][key]['elapse'],data['result'][key]['block'],data['result'][key]['status_code'],data['result'][key]['headers'].replace("'","\\'"),data['result'][key]['text'].replace("'","\\'"),ip)
            cursor.execute(sql)
        cursor.close()
        self.db.commit()

    # 获取所有节点最近num个时间区块的监测结果
    def get_data(self,num):
        self.test_conn()
        cursor = self.db.cursor()
        # 获取7天内数据
        sql = "SELECT detect.rpc_url,detect.detect_time,detect.result,rpc.type,rpc.register,rpc.name from detect,rpc where DATE_SUB(CURDATE(),INTERVAL 7 DAY )<= date(detect_time) and rpc_url=url ORDER BY rpc_url,detect_time DESC"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        record = {}
        for i in result:
            if i[0] not in record.keys():
                record[i[0]] = []
            record[i[0]].append({"result":i[2],"time":i[1],"type":i[3],"register":i[4],"name":i[5]})
        # 监测数据转化为颜色等级
        now_time = time.strftime('%Y-%m-%d %H:%M:%S')
        now_time_struct = datetime.datetime.strptime(now_time, "%Y-%m-%d %H:%M:%S")
        color = {}
        for i in record.keys():
            if i not in color.keys():
                color[i] = {"color":[],"type":record[i][0]["type"],"register":record[i][0]["register"],"name":record[i][0]["name"]}
            sum = 0     # 失败的次数
            all = 0     # 总监测的次数
            last_block = -1
            for j in range(len(record[i])):
                seconds = (now_time_struct-datetime.datetime.strptime(str(record[i][j]["time"]), "%Y-%m-%d %H:%M:%S")).total_seconds()
                time_block = int(seconds/self.block_time_len)
                if record[i][j]["result"]==0:
                    sum+=1
                all+=1
                if last_block==-1:
                    last_block = time_block
                elif last_block!=time_block or j==len(record[i])-1:
                    if sum<=int(self.green*all):
                        color[i]["color"].append('green')
                    elif sum<=int(self.yellow*all):
                        color[i]["color"].append('yellow')
                    else:
                        color[i]["color"].append('red')
                    sum = 0
                    all = 0
                    if len(color[i]["color"])==num:  #数量够了
                        break
                last_block = time_block
            if len(color[i]["color"])<num:          # 监测数据不足则填充null
                color[i]["color"].extend(['null']*(num-len(color[i]["color"])))
        return color

    # 增加rpc节点
    def add_rpc(self,rpcs):
        self.test_conn()
        cursor = self.db.cursor()
        for url in rpcs:
            sql = "insert into config(rpc_url) values('%s')" % (url)
            cursor.execute(sql)
        cursor.close()
        self.db.commit()



 
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

@app.route("/get_data",methods=["GET"]) 
def get_data():
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
    except:
        return {"status":"fail"}

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
