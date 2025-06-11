from flask import Flask, jsonify, request, render_template
import json
import os
from datetime import datetime
import logging
import threading
import time
import requests

app = Flask(__name__)

# 配置文件路径
CONFIG_FILE = 'producer_config.json'

# 日志配置
logging.basicConfig(
    filename='controller.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "global_enabled": True,
        "producers": {}
    }

def save_config(config):
    """保存配置文件"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

@app.route('/')
def index():
    """主页"""
    config = load_config()
    return render_template('index.html', config=config)

@app.route('/producer/connect', methods=['POST'])
def connect_producer():
    """处理生产者连接请求"""
    try:
        data = request.json
        if not data:
            logger.error("请求数据为空")
            return jsonify({"error": "请求数据为空"}), 400
            
        producer_ip = data.get('producer_ip')
        if not producer_ip:
            logger.error("缺少producer_ip参数")
            return jsonify({"error": "缺少producer_ip参数"}), 400
        
        config = load_config()
        if producer_ip not in config["producers"]:
            config["producers"][producer_ip] = {
                "status": "connected",
                "last_heartbeat": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "instances": {}
            }
            save_config(config)
            logger.info(f"新生产者连接: {producer_ip}")
        
        return jsonify({"message": "连接成功"})
    except Exception as e:
        logger.error(f"处理连接请求时发生错误: {str(e)}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@app.route('/producer/heartbeat', methods=['POST'])
def heartbeat():
    """处理生产者心跳"""
    data = request.json
    producer_ip = data.get('producer_ip')
    
    if not producer_ip:
        return jsonify({"error": "缺少producer_ip参数"}), 400
    
    config = load_config()
    if producer_ip in config["producers"]:
        config["producers"][producer_ip]["last_heartbeat"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_config(config)
        logger.info(f"收到心跳: {producer_ip}")
        return jsonify({"message": "心跳成功"})
    
    return jsonify({"error": "生产者未注册"}), 404

@app.route('/producer/create', methods=['POST'])
def create_producer():
    """创建新的生产者实例"""
    data = request.json
    producer_ip = data.get('producer_ip')
    producer_id = data.get('producer_id')
    kafka_server = data.get('kafka_server')
    topic = data.get('topic')
    date_range = data.get('date_range')
    
    if not all([producer_ip, producer_id, kafka_server, topic, date_range]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    config = load_config()
    if producer_ip not in config["producers"]:
        return jsonify({"error": "生产者未注册"}), 404
    
    if producer_id in config["producers"][producer_ip]["instances"]:
        return jsonify({"error": "生产者ID已存在"}), 400
    
    config["producers"][producer_ip]["instances"][producer_id] = {
        "kafka_server": kafka_server,
        "topic": topic,
        "date_range": date_range,
        "status": "running",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    save_config(config)
    logger.info(f"创建生产者实例: {producer_ip}/{producer_id}")
    
    return jsonify({"message": "创建成功"})

@app.route('/producer/pause', methods=['POST'])
def pause_producer():
    """暂停生产者实例"""
    data = request.json
    producer_ip = data.get('producer_ip')
    producer_id = data.get('producer_id')
    
    if not all([producer_ip, producer_id]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    config = load_config()
    if producer_ip not in config["producers"]:
        return jsonify({"error": "生产者未注册"}), 404
    
    if producer_id not in config["producers"][producer_ip]["instances"]:
        return jsonify({"error": "生产者实例不存在"}), 404
    
    config["producers"][producer_ip]["instances"][producer_id]["status"] = "paused"
    save_config(config)
    logger.info(f"暂停生产者实例: {producer_ip}/{producer_id}")
    
    return jsonify({"message": "暂停成功"})

@app.route('/producer/delete', methods=['POST'])
def delete_producer():
    """删除生产者实例"""
    data = request.json
    producer_ip = data.get('producer_ip')
    producer_id = data.get('producer_id')
    
    if not all([producer_ip, producer_id]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    config = load_config()
    if producer_ip not in config["producers"]:
        return jsonify({"error": "生产者未注册"}), 404
    
    if producer_id not in config["producers"][producer_ip]["instances"]:
        return jsonify({"error": "生产者实例不存在"}), 404
    
    del config["producers"][producer_ip]["instances"][producer_id]
    save_config(config)
    logger.info(f"删除生产者实例: {producer_ip}/{producer_id}")
    
    return jsonify({"message": "删除成功"})

@app.route('/status')
def get_status():
    """获取当前状态"""
    config = load_config()
    return jsonify(config)

if __name__ == '__main__':
    # 确保配置文件存在
    if not os.path.exists(CONFIG_FILE):
        save_config({
            "global_enabled": True,
            "producers": {}
        })
    
    app.run(host='0.0.0.0', port=5000) 