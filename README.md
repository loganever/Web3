# slave需要安装的python库
grequests

# master需要安装的python库
pymysql
flask


# 启动方式
master节点启动方式 nohup python master.py -c config文件位置 &

slave节点启动方式 nohup python slave.py -H http://ip:port -t 监测时间间隔(秒) &

# master调用接口示例
获得过去num小时的监测数据
curl http://127.0.0.1:5000/get_data?num=24

获得配置的所有rpc节点
curl http://127.0.0.1:5000/config
