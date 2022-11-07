# slave需要安装的python库
grequests

# master需要安装的python库
pymysql
flask


# 启动方式
先启动 master,再启动slave,可部署多个slave

master配置项：数据库连接配置，颜色等级配置，每小时检测次数

slave配置项：master的ip及端口，监测时间间隔(3600/master的每小时检测次数)

master节点启动方式 nohup python master.py -c config文件位置

slave节点启动方式 nohup python slave.py -H http://ip:port -t 监测时间间隔(秒) &

# master调用接口示例
获得过去num小时的监测数据
curl http://127.0.0.1:5000/get_data?num=3

增加rpc节点
curl -X POST -H 'Content-Type: application/json' -d '{"rpcs": ["aaa", "bbb"] }' http://127.0.0.1:5000/add_rpc

获得配置的所有rpc节点
curl http://127.0.0.1:5000/config

获得私有rpc节点名称及注册链接
curl http://127.0.0.1:5000/private
