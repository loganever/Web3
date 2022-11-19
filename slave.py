import grequests
import requests
import json
import time
import datetime
from optparse import OptionParser
import logging
from multiprocessing import Process 

# 日志设置
logging.basicConfig(filename='slave_log.txt', level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

class Slave:

    def __init__(self,master_node_url, node_name):
        self.master_node_url = master_node_url
        self.node_name = node_name
        self.rpc_version = 0
        self.code_version = 0
        self.req_list = self.build_requests()
        self.result = {}
        self.newest = {}
        self.reload_flag = False

    # 设置grequests请求列表
    def build_requests(self):
        result = json.loads(requests.get(self.master_node_url+'/config').text.strip())
        rpc_nodes = result['rpc']
        chains = result['chain']
        payloads = result['payload']
        self.rpc_version = result['rpc_version']
        self.code_version = result['code_version']
        headers = {'Content-type': 'application/json'}
        req_list = {}
        for i in range(len(rpc_nodes)):
            if chains[i] not in req_list.keys():
                req_list[chains[i]] = [] 
            req_list[chains[i]].append(grequests.post(rpc_nodes[i], json=json.loads(payloads[i]), headers=headers,timeout=3))
        return req_list

    # 请求错误处理
    def err_handler(self,request, exception):
        self.result[request.url] = {"text":str(exception)}

    # 监测某一条链
    def detect(self,chain):
        self.result = {}
        self.newest[chain] = 0
        # 获得所有请求结果
        res_list = grequests.map(self.req_list[chain], exception_handler=self.err_handler)
        for res,req in zip(res_list,self.req_list[chain]):
            try:
                resp = json.loads(res.text.strip())        
                infura_block_number = int(resp[0]['result'],16)   # 结果16进制转10进制
                # evm_block_numbser = int(resp[1]['result'][60:66],16)          #获得evm返回的区块号
                self.result[req.url] = {"block":infura_block_number,"elapse":res.elapsed.total_seconds(),
                "status_code":res.status_code,"headers":str(res.headers),"text":res.text.strip(),"chain":chain}
                self.newest[chain] = max(self.newest[chain],self.result[req.url]['block'])
            except Exception as e:
                if res!=None:
                    self.result[req.url] = {"block":-1,"elapse":res.elapsed.total_seconds(),"status_code":res.status_code,"headers":str(res.headers),"text":res.text.strip(),"chain":chain}
                else:
                    self.result[req.url]["block"] = -1
                    self.result[req.url]["elapse"] = 0.0
                    self.result[req.url]["status_code"] = -1
                    self.result[req.url]["headers"] = ""
                    self.result[req.url]["chain"] = chain
        self.post_to_master(chain)

    def post_to_master(self,chain):
        try:
            data = {"result":self.result, "newest":self.newest[chain], "node_name":self.node_name}
            headers = {'content-type': "application/json"}
            response = requests.post(self.master_node_url+'/recive', json=data, timeout=5, headers = headers)
            result = json.loads(response.text)
            if result['code_version']!=self.code_version:
                self.reload_flag = True
            if result['rpc_version']!=self.rpc_version:
                self.req_list = self.build_requests()
        except Exception as e:
            logging.info(e)
            logging.info("post error")
