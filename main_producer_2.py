import argparse
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import json
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import time
import requests
import socket
import sys
from flask import Flask, jsonify, request
import threading

# 创建Flask应用
app = Flask(__name__)

# 创建全局生产者实例
producer = None

def get_local_ip():
    """获取本机IP地址
    用于向控制器标识自己，使控制器能够识别和管理不同的生产者
    如果获取失败则返回本地回环地址127.0.0.1
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

class RemoteControlledProducer:
    """远程控制的生产者类
    相比原来的KafkaEventProducer，增加了远程控制功能
    可以通过控制器动态控制运行状态和日期范围
    """
    def __init__(self, controller_url):
        self.controller_url = controller_url
        self.local_ip = get_local_ip()
        self.producers = {}  # 存储所有生产者实例
        self.logger = self._setup_logger()
        self.connected = False
        self.heartbeat_thread = None

    def _setup_logger(self):
        """设置日志记录器"""
        logger = logging.getLogger('RemoteProducer')
        logger.setLevel(logging.INFO)
        
        # 文件处理器 - 记录到producer.log
        fh = logging.FileHandler('producer.log')
        fh.setLevel(logging.INFO)
        
        # 控制台处理器 - 实时显示日志
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 格式化器 - 包含时间、名称、级别和消息
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

    def connect_to_controller(self):
        """连接到控制器"""
        try:
            response = requests.post(
                f"{self.controller_url}/producer/connect",
                json={"producer_ip": self.local_ip}
            )
            if response.status_code == 200:
                self.connected = True
                self.logger.info("成功连接到控制器")
                # 启动心跳线程
                self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
                self.heartbeat_thread.daemon = True
                self.heartbeat_thread.start()
                return True
            else:
                self.logger.error(f"连接控制器失败: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"连接控制器失败: {str(e)}")
            return False

    def _heartbeat_loop(self):
        """心跳循环"""
        while self.connected:
            try:
                response = requests.post(
                    f"{self.controller_url}/producer/heartbeat",
                    json={"producer_ip": self.local_ip}
                )
                if response.status_code != 200:
                    self.logger.error("心跳失败")
                    self.connected = False
                    break
            except Exception as e:
                self.logger.error(f"心跳失败: {str(e)}")
                self.connected = False
                break
            time.sleep(30)  # 每30秒发送一次心跳

    def create_producer(self, producer_id, kafka_server, topic, date_range):
        """创建新的生产者"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=524288,
                linger_ms=1000,
                compression_type='gzip',
                max_request_size=4194304,
                buffer_memory=134217728,
                acks='all',
                retries=5,
                request_timeout_ms=45000
            )
            
            self.producers[producer_id] = {
                "producer": producer,
                "kafka_server": kafka_server,
                "topic": topic,
                "date_range": date_range,
                "status": "running",
                "thread": None
            }
            
            # 启动生产者线程
            thread = threading.Thread(
                target=self._producer_loop,
                args=(producer_id,)
            )
            thread.daemon = True
            thread.start()
            self.producers[producer_id]["thread"] = thread
            
            self.logger.info(f"创建生产者成功: {producer_id}")
            return True
        except Exception as e:
            self.logger.error(f"创建生产者失败: {str(e)}")
            return False

    def _producer_loop(self, producer_id):
        """生产者主循环"""
        producer_info = self.producers[producer_id]
        start_date = datetime.strptime(producer_info["date_range"]["start"], "%Y%m%d")
        end_date = datetime.strptime(producer_info["date_range"]["end"], "%Y%m%d")
        
        current_date = start_date
        while current_date <= end_date and producer_info["status"] == "running":
            try:
                self._produce_events(producer_id, current_date)
                current_date += timedelta(days=1)
            except Exception as e:
                self.logger.error(f"生产事件失败: {str(e)}")
                time.sleep(60)  # 发生错误时等待一分钟

    def _produce_events(self, producer_id, date):
        """生产事件到Kafka"""
        producer_info = self.producers[producer_id]
        # 如需采集真实GitHub事件，取消下行注释
        # events = get_github_events(date, [
        #     "PushEvent", "ForkEvent", "PullRequestEvent",
        #     "IssuesEvent", "CreateEvent", "WatchEvent",
        #     "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
        #     "IssueCommentEvent"
        # ])
        # 当前为模拟数据，便于开发和测试
        events = [{"type": "PushEvent", "date": date.strftime("%Y%m%d")}]

        success_count = 0
        for event in tqdm(events, desc=f"处理 {date} 的事件"):
            try:
                cleaned_event = self._clean_event(dict(event))
                producer_info["producer"].send(
                    topic=producer_info["topic"],
                    value=cleaned_event
                )
                success_count += 1
                if success_count % 1000 == 0:
                    producer_info["producer"].flush()

            except KafkaTimeoutError as e:
                self.logger.error(f"发送超时: {str(e)}")
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"处理异常: {str(e)}")
                continue

        producer_info["producer"].flush()
        self.logger.info(f"✅ 成功生产 {date} 的事件到 {producer_info['topic']}")

    def _clean_value(self, value):
        """类型安全转换"""
        if isinstance(value, bool):
            return 1 if value else 0
        elif isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, datetime):
            return int(value.timestamp())
        elif isinstance(value, (str, bytes, int, float)):
            return value
        else:
            return str(value)

    def _clean_event(self, event):
        """递归清理整个事件结构"""
        if isinstance(event, dict):
            return {k: self._clean_value(v) for k, v in event.items()}
        elif isinstance(event, list):
            return [self._clean_value(v) for v in event]
        else:
            return self._clean_value(event)

    def pause_producer(self, producer_id):
        """暂停生产者"""
        if producer_id in self.producers:
            self.producers[producer_id]["status"] = "paused"
            self.logger.info(f"生产者已暂停: {producer_id}")
            return True
        return False

    def delete_producer(self, producer_id):
        """删除生产者"""
        if producer_id in self.producers:
            producer_info = self.producers[producer_id]
            producer_info["status"] = "stopped"
            if producer_info["producer"]:
                producer_info["producer"].close()
            del self.producers[producer_id]
            self.logger.info(f"生产者已删除: {producer_id}")
            return True
        return False

# Flask路由处理
@app.route('/producer/create', methods=['POST'])
def handle_create():
    data = request.json
    producer_id = data.get('producer_id')
    kafka_server = data.get('kafka_server')
    topic = data.get('topic')
    date_range = data.get('date_range')
    
    if not all([producer_id, kafka_server, topic, date_range]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    if producer.create_producer(producer_id, kafka_server, topic, date_range):
        return jsonify({"message": "生产者创建成功"})
    else:
        return jsonify({"error": "创建生产者失败"}), 500

@app.route('/producer/pause', methods=['POST'])
def handle_pause():
    data = request.json
    producer_id = data.get('producer_id')
    
    if not producer_id:
        return jsonify({"error": "缺少producer_id参数"}), 400
    
    if producer.pause_producer(producer_id):
        return jsonify({"message": "生产者已暂停"})
    else:
        return jsonify({"error": "暂停生产者失败"}), 500

@app.route('/producer/delete', methods=['POST'])
def handle_delete():
    data = request.json
    producer_id = data.get('producer_id')
    
    if not producer_id:
        return jsonify({"error": "缺少producer_id参数"}), 400
    
    if producer.delete_producer(producer_id):
        return jsonify({"message": "生产者已删除"})
    else:
        return jsonify({"error": "删除生产者失败"}), 500

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub 事件爬虫（远程控制版本）")
    parser.add_argument("--controller-url", required=True, help="控制器URL，例如：http://controller-ip:5000")
    args = parser.parse_args()

    # 创建生产者实例
    producer = RemoteControlledProducer(args.controller_url)
    
    # 连接到控制器
    if not producer.connect_to_controller():
        print("无法连接到控制器，程序退出")
        sys.exit(1)
    
    # 启动Flask应用
    app.run(host='0.0.0.0', port=5001)