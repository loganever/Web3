import slave
import requests
from importlib import reload
import logging
import time
from optparse import OptionParser

# 日志设置
logging.basicConfig(filename='slave_log.txt', level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


if __name__=="__main__":
    parser = OptionParser()

    parser.add_option("-H", "--host", help="Master节点ip端口")
    parser.add_option("-t", "--time", help="总监测时间间隔")
    parser.add_option("-n", "--name", help="slave节点名称")
    (options, args) = parser.parse_args()
    time_len = int(options.time)

    slave_node = slave.Slave(options.host, options.name)
    chains = []
    for chain in slave_node.req_list.keys():
        chains.append(chain)
    while True:
        for chain in chains:
            slave_node.detect(chain)
        if slave_node.reload_flag:
            try:
                logging.info(options.name+" reload begin")
                code = requests.get('https://raw.githubusercontent.com/loganever/Web3/main/slave.py').text
                with open("slave.py" ,"w", encoding='utf8') as f:
                    f.write(code)
                reload(slave)
                slave_node = slave.Slave(options.host, options.name)
                chains = []
                for chain in slave_node.req_list.keys():
                    chains.append(chain)
                logging.info(options.name+" reload success")
            except Exception as e:
                logging.error(e)
        time.sleep(time_len)
