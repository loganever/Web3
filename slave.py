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
        self.req_list = self.build_requests()
        self.result = {}
        self.newest = 0
        self.rpc_version = 0
        self.code_version = 0
        self.reload_flag = False

    # 设置grequests请求列表
    def build_requests(self):
        result = json.loads(requests.get(self.master_node_url+'/config').text.strip())
        rpc_nodes = result['rpc']
        self.rpc_version = result['rpc_version']
        headers = {'Content-type': 'application/json'}
        payload = [{"jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1},
                {"jsonrpc":"2.0","id":2,"method":"eth_call","params":[
                    {"to":"0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441",
                    "data":"0x252dba42000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
                    "latest"]}]
        req_list = []
        for url in rpc_nodes:
            req_list.append(grequests.post(url, json=payload, headers=headers,timeout=3))
        return req_list

    # 请求错误处理
    def err_handler(self,request, exception):
        self.result[request.url] = {"text":str(exception)}

    # 获得区块号
    def get_block_num(self):
        self.result = {}
        self.newest = 0
        # 获得所有请求结果
        res_list = grequests.map(self.req_list, exception_handler=self.err_handler)
        for res,req in zip(res_list,self.req_list):
            try:
                resp = json.loads(res.text.strip())        
                infura_block_number = int(resp[0]['result'],16)   # 结果16进制转10进制
                evm_block_numbser = int(resp[1]['result'][60:66],16)          #获得evm返回的区块号
                self.result[req.url] = {"block":max(infura_block_number,evm_block_numbser),"elapse":res.elapsed.total_seconds(),
                "status_code":res.status_code,"headers":str(res.headers),"text":res.text.strip()}
                self.newest = max(self.newest,self.result[req.url]['block'])
            except Exception as e:
                if res!=None:
                    self.result[req.url] = {"block":-1,"elapse":res.elapsed.total_seconds(),"status_code":res.status_code,"headers":str(res.headers),"text":res.text.strip()}
                else:
                    self.result[req.url]["block"] = -1
                    self.result[req.url]["elapse"] = 0.0
                    self.result[req.url]["status_code"] = -1
                    self.result[req.url]["headers"] = ""
        return self.result

    def post_to_master(self):
        try:
            data = {"result":self.result, "newest":self.newest, "node_name":self.node_name}
            headers = {'content-type': "application/json"}
            response = requests.post(self.master_node_url+'/recive', json=data, timeout=5, headers = headers)
            result = json.loads(response.text)
            if result['code_version']!=self.code_version:
                self.reload_flag = True
            if result['rpc_version']!=self.rpc_version:
                self.req_list = self.build_requests()
        except Exception as e:
            logging.info("post error")
