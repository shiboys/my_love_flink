import random
import json
import time
from datetime import datetime

# kafka 连接器的示例数据源产生器

# 使用方式如下：
# sshuser@hn0-contsk:~$ python kafka_connector_producer.py | /usr/kafka_path/kafka-broker/bin/kafka-console-producer.sh --bootstrap-server wn0-contsk:9092 --topic click_events

user_set=[
    'John',
        'XiaoMing',
        'Mike',
        'Tom',
        'Machael',
        'Zheng Hu',
        'Zark',
        'Tim',
        'Andrew',
        'Peter',
        'Sean',
        'Luke',
        'Chunck'
]

web_set=[
    'https://google.com',
        'https://facebook.com?id=1',
        'https://tmall.com',
        'https://baidu.com',
        'https://taobao.com',
        'https://aliyun.com',
        'https://apache.com',
        'https://flink.apache.com',
        'https://hbase.apache.com',
        'https://github.com',
        'https://gmail.com',
        'https://stackoverflow.com',
        'https://python.org'
]

def main() :
    while True:
        if random.randrange(10) < 4 :
            url = random.choice(web_set[:3])
        else :
            url = random.choice(web_set)
        
        log_entry = {
            "userName": random.choice(user_set),
            "visitUrl":url,
            "ts":datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        }
        
        print(json.dumps(log_entry))
        time.sleep(0.05)
        
if __name__ == "__main__":
    main()
    
        