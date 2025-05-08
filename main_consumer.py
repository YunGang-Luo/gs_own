import logging
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from multiprocessing import Process
from tu_util import *
from es_util import *
import argparse


def safe_get(data, *keys):
    """安全获取嵌套字典值"""
    for key in keys:
        if key == "payload":
            key = "event_payload"
        try:
            if isinstance(data, str):
                data = json.loads(data)
            data = data[key]
        except (KeyError, TypeError):
            return None
    return data


def org_make(event):
    org_id = safe_get(event, "org_id")
    repo_id = safe_get(event, "repo_id")
    if org_id:
        create_node("github_organization", {"id": org_id, "name": safe_get(event, "org_login")})
    if repo_id and org_id:
        create_relationship({"label": "github_organization", "properties": {"id": org_id}},
                            {"label": "github_repo", "properties": {"id": repo_id}}, "has_repo", {})

def user_make(event, kafka_producer):
    """用户数据发送到github_users主题"""
    user_id = safe_get(event, "actor_id")
    if user_id:
        new_user = {"id": user_id, "name": safe_get(event, "actor_login"), "flag": -1}
        new_user_tu = {"id": user_id, "name": safe_get(event, "actor_login")}
        if create_user(new_user):
            create_node("github_user", new_user_tu)
            # 发送到用户主题，使用用户ID作为消息键
            kafka_producer.send(
                topic='github_users',
                value=new_user,
                key=str(user_id)
            )
            kafka_producer.flush()  # 确保立即发送

def repo_make(event, kafka_producer):
    """仓库数据发送到github_repos主题"""
    repo_id = safe_get(event, "repo_id")
    if repo_id:
        new_repo = {
            "id": repo_id,
            "name": safe_get(event, "repo_name"),
            "star": 0,
            "flag": -1
        }
        new_repo_tu = {
            "id": repo_id,
            "name": safe_get(event, "repo_name"),
            "star": 0
        }
        if create_repo(new_repo):
            create_node("github_repo", new_repo_tu)
            # 发送到仓库主题，使用仓库ID作为消息键
            kafka_producer.send(
                topic='github_repos',
                value=new_repo,
                key=str(repo_id)
            )
            kafka_producer.flush()


class StreamConsumer:
    def __init__(self, consumer_name, topic='github_events',
                 group_id='event-group', bootstrap_servers='127.0.0.1:9092'):
        self.consumer_name = consumer_name
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.handlers = {}
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()

        # 日志配置
        logging.basicConfig(
            filename='kafka_errors.log',
            level=logging.ERROR,
            format='%(asctime)s %(name)s ERROR: %(message)s'
        )
        self.logger = logging.getLogger(consumer_name)

        # 事件处理器（示例）
        self.handlers = {
            "PushEvent": self.handle_pushevent,
            "ForkEvent": self.handle_forkevent,
            "PullRequestEvent": self.handle_pullrequest,
            "WatchEvent": self.handle_watchevent,
            "IssuesEvent": self.handle_issuesevent,
            "IssueCommentEvent": self.handle_issuecommentevent,
            "CreateEvent": self.handle_createevent,
            "PullRequestReviewCommentEvent": self.handle_pullrequestcommentevent,
            "PullRequestReviewEvent": self.handle_pullrequestreviewevent,
        }

    def _create_consumer(self):
        """创建并配置Kafka消费者"""
        retries = 5
        while retries > 0:
            try:
                return KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    max_poll_records=500,
                    session_timeout_ms=30000,
                    max_poll_interval_ms=300000
                )
            except NoBrokersAvailable:
                print("等待Kafka broker可用...")
                time.sleep(5)
                retries -= 1
        raise RuntimeError("无法连接Kafka broker")

    def _create_producer(self, bootstrap_servers='127.0.0.1:9092'):
        """创建Kafka生产者"""
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: str(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # 确保消息可靠投递
            retries=3  # 失败重试次数
        )

    def handle_pushevent(self, event):
        """处理PushEvent"""
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if user_id:
            user_make(event, self.producer)
        if repo_id:
            repo_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "github_repo", "properties": {"id": repo_id}}, "push",
                            {"created_at": iso_to_int32(event["created_at"]), "commits": 1})

    def handle_forkevent(self, event):
        """处理ForkEvent"""
        user_id = safe_get(event, "actor_id")
        fork_repo = safe_get(event, "payload", "forkee")
        fork_repo = json.loads(fork_repo)
        if user_id:
            user_make(event, self.producer)
        if fork_repo:
            repo_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "github_repo", "properties": {"id": fork_repo["id"]}}, "fork",
                            {"created_at": iso_to_int32(event["created_at"])})

    def handle_pullrequest(self, event):
        """处理PullRequestEvent"""
        pr_data = safe_get(event, "payload", "pull_request")
        if not pr_data:
            return

        pr_data = json.loads(pr_data)
        pr_id = pr_data["id"]
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if pr_id:
            create_node("pr",
                        {"id": str(pr_id), "additions": pr_data["additions"], "changed_files": pr_data["changed_files"],
                         "created_at": iso_to_int32(pr_data["created_at"]), "deletions": pr_data["deletions"],
                         "merged": pr_data["merged"]})

        if user_id:
            user_make(event, self.producer)

        if repo_id:
            repo_make(event, self.producer)

        create_relationship({"label": "github_repo", "properties": {"id": repo_id}},
                            {"label": "pr", "properties": {"id": str(pr_id)}}, "has_pr",
                            {"created_at": iso_to_int32(event["created_at"])})

        action = safe_get(event, "payload", "action")
        if action == "opened":
            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "pr", "properties": {"id": str(pr_id)}}, "open_pr",
                                {"created_at": iso_to_int32(event["created_at"])})
        elif action == "closed":
            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "pr", "properties": {"id": str(pr_id)}}, "close_pr",
                                {"created_at": iso_to_int32(event["created_at"])})
        elif action == "reopened":
            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "pr", "properties": {"id": str(pr_id)}}, "open_pr",
                                {"created_at": iso_to_int32(event["created_at"])})

        org_make(event)

    def handle_pullrequestreviewevent(self, event):
        """处理PullRequestReviewEvent"""
        pr_id = safe_get(event, "payload", "pull_request", "id")
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if pr_id:
            create_node("pr",
                        {"id": str(pr_id), "additions": 0, "changed_files": 0,
                         "created_at": iso_to_int32(safe_get(event, "payload", "pull_request", "created_at")),
                         "deletions": 0, "merged": False})

        if user_id:
            user_make(event, self.producer)

        if repo_id:
            repo_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "pr", "properties": {"id": str(pr_id)}}, "review_pr",
                            {"created_at": iso_to_int32(event["created_at"])})
        create_relationship({"label": "github_repo", "properties": {"id": repo_id}},
                            {"label": "pr", "properties": {"id": str(pr_id)}}, "has_pr",
                            {"created_at": iso_to_int32(safe_get(event, "payload", "pull_request", "created_at"))})

        org_make(event)

    def handle_pullrequestcommentevent(self, event):
        """处理PullRequestReviewCommentEvent"""
        pr_id = safe_get(event, "payload", "pull_request", "id")
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if pr_id:
            create_node("pr",
                        {"id": str(pr_id), "additions": 0, "changed_files": 0,
                         "created_at": iso_to_int32(safe_get(event, "payload", "pull_request", "created_at")),
                         "deletions": 0, "merged": False})

        if user_id:
            user_make(event, self.producer)

        if repo_id:
            repo_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "pr", "properties": {"id": str(pr_id)}}, "comment_pr",
                            {"created_at": iso_to_int32(event["created_at"])})
        create_relationship({"label": "github_repo", "properties": {"id": repo_id}},
                            {"label": "pr", "properties": {"id": str(pr_id)}}, "has_pr",
                            {"created_at": iso_to_int32(safe_get(event, "payload", "pull_request", "created_at"))})

        org_make(event)

    def handle_watchevent(self, event):
        """处理WatchEvent（star操作）"""
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if user_id:
            user_make(event, self.producer)
        if repo_id:
            repo_make(event, self.producer)

        org_make(event)

    def handle_issuesevent(self, event):
        """处理IssuesEvent"""
        issue_data = safe_get(event, "payload", "issue")
        if not issue_data:
            return
        issue_data = json.loads(issue_data)
        issue_id = issue_data["id"]
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")
        action = safe_get(event, "payload", "action")

        if repo_id:
            repo_make(event, self.producer)

        if user_id:
            user_make(event, self.producer)

        if action == "closed":
            if create_node("issue", {"id": str(issue_id), "state": issue_data["state"],
                                     "closed_at": iso_to_int32(event["created_at"]),
                                     "created_at": iso_to_int32(
                                         safe_get(event, "payload", "issue", "created_at"))}) is None:
                update_node("issue", match_props={"id": str(issue_id)},
                            set_props={"state": issue_data["state"], "closed_at": iso_to_int32(event["created_at"])})

            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "issue", "properties": {"id": str(issue_id)}}, "close_issue",
                                {"created_at": iso_to_int32(event["created_at"])})

        elif action == "opened":
            if create_node("issue", {"id": str(issue_id), "state": issue_data["state"], "created_at": iso_to_int32(
                    safe_get(event, "payload", "issue", "created_at"))}) is None:
                update_node("issue", match_props={"id": str(issue_id)}, set_props={"state": issue_data["state"],
                                                                                   "created_at": iso_to_int32(
                                                                                       safe_get(event, "payload",
                                                                                                "issue",
                                                                                                "created_at"))})

            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "issue", "properties": {"id": str(issue_id)}}, "open_issue",
                                {"created_at": iso_to_int32(event["created_at"])})

        elif action == "reopened":
            if create_node("issue", {"id": str(issue_id), "state": issue_data["state"], "created_at": iso_to_int32(
                    safe_get(event, "payload", "issue", "created_at"))}) is None:
                update_node("issue", match_props={"id": str(issue_id)}, set_props={"state": issue_data["state"]})

            create_relationship({"label": "github_user", "properties": {"id": user_id}},
                                {"label": "issue", "properties": {"id": str(issue_id)}}, "open_issue",
                                {"created_at": iso_to_int32(event["created_at"])})

        create_relationship({"label": "github_repo", "properties": {"id": repo_id}},
                            {"label": "issue", "properties": {"id": str(issue_id)}}, "has_issue",
                            {"created_at": iso_to_int32(event["created_at"])})

        org_make(event)

    def handle_issuecommentevent(self, event):
        """处理IssueCommentEvent"""
        issue_id = safe_get(event, "payload", "issue", "id")
        user_id = safe_get(event, "actor_id")
        repo_id = safe_get(event, "repo_id")

        if issue_id:
            create_node("issue", {"id": str(issue_id), "state": safe_get(event, "payload", "issue", "state"),
                                  "created_at": iso_to_int32(safe_get(event, "payload", "issue", "created_at"))})

        if user_id:
            user_make(event, self.producer)

        if repo_id:
            repo_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "issue", "properties": {"id": str(issue_id)}}, "comment_issue",
                            {"created_at": iso_to_int32(event["created_at"])})
        create_relationship({"label": "github_repo", "properties": {"id": repo_id}},
                            {"label": "issue", "properties": {"id": str(issue_id)}}, "has_issue",
                            {"created_at": iso_to_int32(event["created_at"])})
        org_make(event)

    def handle_createevent(self, event):
        """处理CreateEvent（仓库创建）"""
        repo_id = safe_get(event, "repo_id")
        user_id = safe_get(event, "actor_id")

        if repo_id:
            repo_make(event, self.producer)

        if user_id:
            user_make(event, self.producer)

        create_relationship({"label": "github_user", "properties": {"id": user_id}},
                            {"label": "github_repo", "properties": {"id": repo_id}}, "push",
                            {"created_at": iso_to_int32(event["created_at"]), "commits": 0})
        org_make(event)


    def process_message(self, message):
        """处理单个消息"""
        try:
            # Kafka消息结构解析
            p_data = {
                **message.value
            }
            for i in p_data:
                if p_data[i] == 'None':
                    p_data[i] = None
            event_type = p_data.get('type')

            if handler := self.handlers.get(event_type):
                handler(p_data)
                return True
            else:
                print(f"Unhandled event type: {event_type}")
                return True  # 未知事件仍然提交offset
        except Exception as e:
            self.log_error(e, p_data)
            return False  # 处理失败返回False

    def log_error(self, error, event):
        error_msg = f"{datetime.now()} [{self.consumer_name}] ERROR: {str(error)}\n"
        error_msg += f"Event: {json.dumps(event, indent=2)}\n"
        error_msg += "=" * 50 + "\n"
        with open(f"stream_errors.log", "a", encoding='utf-8') as f:
            f.write(error_msg)

    def consume(self):
        """主消费循环"""
        # 订阅主题
        self.consumer.subscribe([self.topic])

        while True:
            try:
                # 批量拉取消息
                batch = self.consumer.poll(timeout_ms=5000, max_records=500)

                if not batch:
                    print(f"{datetime.now()} {self.consumer_name} 无新消息，等待中...")
                    continue

                # 处理每个分区的消息
                for tp, messages in batch.items():
                    for msg in messages:
                        self.process_message(msg)
            #     输出进程名和处理了多少条数据
                    print(f"{self.consumer_name} 处理了 {len(messages)} 条数据")
            except Exception as e:
                self.log_error(e, {})
                time.sleep(1)


def consumer_worker(worker_id):
    consumer = StreamConsumer(
        consumer_name=f"worker_{worker_id}",
        bootstrap_servers='127.0.0.1:9092'  # 修改为实际地址
    )

    consumer.consume()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub Kafka消费者")
    parser.add_argument('--workers', type=int, default=4, help='Worker 进程数（默认4）')
    args = parser.parse_args()

    NUM_WORKERS = args.workers

    processes = []
    for i in range(NUM_WORKERS):
        p = Process(target=consumer_worker, args=(i,))
        p.start()
        processes.append(p)

    try:
        while any(p.is_alive() for p in processes):
            time.sleep(1)
    except KeyboardInterrupt:
        print("终止信号接收，等待消费者退出...")

    for p in processes:
        p.terminate()
        p.join()

    print("所有消费者已安全退出")