from kafka import KafkaConsumer, KafkaProducer
import json
import multiprocessing
import signal
import time
from github_crawler_v2 import *
from tu_util_2 import update_node, create_node, create_relationship
from es_util import get_repo_by_id, get_user_by_id, update_repo_by_id, update_user_by_id
import argparse

# 配置参数
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "group_id": "github-crawlers",
    "auto_offset_reset": "earliest"
}

def init_worker():
    """初始化工作进程（忽略键盘中断）"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def process_user(data, kafka_producer, token_manager):
    try:
        # 数据已经是反序列化的字典
        old_data = get_user_by_id(data['id'])
        if not old_data:
            return
        if old_data["flag"] != -1:
            return
        old_data["flag"] = 1
        print(f"[进程 {multiprocessing.current_process().name}] 处理用户任务")
        result = get_user_data(data['name'], token_manager)
        if result:
            # 生成更新节点的Cypher语句并发送到Kafka
            cypher = update_node("github_user", {"id": result['id']},
                        {"company": result['company'], "country": result['country']})
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "update_node"}).encode('utf-8')
                )
        update_user_by_id(data['id'], old_data)
    except Exception as e:
        print(f"用户处理失败: {e}")
        print(data)

def process_repo(data, kafka_producer, token_manager):
    """处理仓库数据"""
    try:
        # 数据已经是反序列化的字典
        old_data = get_repo_by_id(data['id'])
        if not old_data:
            return
        if old_data["flag"] != -1:
            return
        old_data["flag"] = 1
        print(f"[进程 {multiprocessing.current_process().name}] 处理仓库任务")
        repo_owner, repo_name = data['name'].split('/')
        result = get_repo_data(repo_owner, repo_name, token_manager)
        if not result:
            return

        # 更新仓库数据 - 发送Cypher语句到Kafka
        cypher = update_node("github_repo", {"id": result['id']}, {
            "star": result['star'],
            "code_additions": result['code_additions'],
            "code_deletions": result['code_deletions'],
            "code_changed_files": result['code_changed_files'],
            "commits": result['commits'],
            "comments": result['comments'],
            "fork": result['fork'],
            "merged_pr": result['merged_pr'],
            "opened_pr": result['opened_pr'],
            "opened_issue": result['opened_issue'],
            "closed_issue": result['closed_issue']
        })
        if cypher:
            kafka_producer.send(
                'cypher',
                value=json.dumps({"cypher": cypher, "type": "update_node"}).encode('utf-8')
            )
        old_data["star"] = result['star']

        # 处理关联数据（语言/许可证/主题/组织）
        if result["language"]:
            cypher = create_node("language", {"name": result["language"]})
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_node"}).encode('utf-8')
                )
            
            cypher = create_relationship(
                {"label": "github_repo", "properties": {"id": result['id']}},
                {"label": "language", "properties": {"name": result["language"]}},
                "use_lang", {}
            )
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_relationship"}).encode('utf-8')
                )

        if result["license"]:
            cypher = create_node("license", {"name": result["license"]})
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_node"}).encode('utf-8')
                )
            
            cypher = create_relationship(
                {"label": "github_repo", "properties": {"id": result['id']}},
                {"label": "license", "properties": {"name": result["license"]}},
                "use_license", {}
            )
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_relationship"}).encode('utf-8')
                )

        for topic in result["topics"]:
            cypher = create_node("topic", {"name": topic})
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_node"}).encode('utf-8')
                )
            
            cypher = create_relationship(
                {"label": "github_repo", "properties": {"id": result['id']}},
                {"label": "topic", "properties": {"name": topic}},
                "has_topic", {}
            )
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_relationship"}).encode('utf-8')
                )

        if result["github_organization"]:
            cypher = create_node("github_organization", {
                "name": result["github_organization"][0],
                "id": result["github_organization"][1]
            })
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_node"}).encode('utf-8')
                )
            
            cypher = create_relationship(
                {"label": "github_organization", "properties": {"id": result["github_organization"][1]}},
                {"label": "github_repo", "properties": {"id": result['id']}},
                "has_repo", {}
            )
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_relationship"}).encode('utf-8')
                )

        # 处理标星用户
        for star_user in result["starred_users"]:
            cypher = create_node("github_user", {"name": star_user[0], "id": star_user[1]})
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_node"}).encode('utf-8')
                )
                # 发送到Kafka的user主题
                kafka_producer.send(
                    'github_users',
                    value=json.dumps({"name": star_user[0], "id": star_user[1]}).encode('utf-8')
                )
            
            cypher = create_relationship(
                {"label": "github_user", "properties": {"id": star_user[1]}},
                {"label": "github_repo", "properties": {"id": result['id']}},
                "star", {"created_at": 0}
            )
            if cypher:
                kafka_producer.send(
                    'cypher',
                    value=json.dumps({"cypher": cypher, "type": "create_relationship"}).encode('utf-8')
                )

        update_repo_by_id(data['id'], old_data)
    except Exception as e:
        print(f"仓库处理失败: {e}")
        print(data)


def worker(token_sublist):
    """工作进程主函数"""
    token_manager = TokenManager(token_sublist)
    # 每个进程独立创建Kafka消费者和生产者
    consumer = KafkaConsumer(
        'github_users', 'github_repos',
        **KAFKA_CONFIG,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda x: x
    )

    try:
        for message in consumer:
            try:
                topic = message.topic
                data = message.value

                if topic == 'github_users':
                    # print(f"[进程 {multiprocessing.current_process().name}] 处理用户任务")
                    process_user(data, producer, token_manager)
                elif topic == 'github_repos':
                    # print(f"[进程 {multiprocessing.current_process().name}] 处理仓库任务")
                    process_repo(data, producer, token_manager)

                # 手动提交偏移量
                consumer.commit()

            except Exception as e:
                print(f"消息处理失败: {e}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub api 处理")
    parser.add_argument('--workers', type=int, default=2, help='Worker 进程数（默认4）')
    args = parser.parse_args()

    NUM_PROCESSES = args.workers

    # 如果tokens.json不存在，报错
    try:
        with open("tokens.json") as f:
            all_tokens = json.load(f)
    except FileNotFoundError:
        print("tokens.json 文件不存在，请确保文件存在并包含有效的令牌列表。")
        exit(1)

    assert len(all_tokens) >= NUM_PROCESSES, "Token数不足以分配给每个Worker"

    token_chunks = [all_tokens[i::NUM_PROCESSES] for i in range(NUM_PROCESSES)]
    
    pool = []
    for i in range(NUM_PROCESSES):
        p = multiprocessing.Process(target=worker, args=(token_chunks[i],), name=f"Process-{i + 1}")
        p.daemon = True
        p.start()
        pool.append(p)

    # 主进程监控
    try:
        for p in pool:
            p.join()
    except KeyboardInterrupt:
        print("\n接收到终止信号，清理进程...")
        for p in pool:
            p.terminate()
        for p in pool:
            p.join()