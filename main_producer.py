import argparse
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from gh_bigquery import get_github_events
import logging
import time


class KafkaEventProducer:
    def __init__(self, topic='github_events', bootstrap_servers='127.0.0.1:9092'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._create_producer()

        # 日志配置
        logging.basicConfig(
            filename='producer_errors.log',
            level=logging.ERROR,
            format='%(asctime)s %(name)s ERROR: %(message)s'
        )
        self.logger = logging.getLogger('KafkaProducer')

    def _create_producer(self):
        """创建并配置Kafka生产者（带重试机制）"""
        retries = 5
        while retries > 0:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    # 大消息专用配置
                    batch_size=524288,  # 512KB批次
                    linger_ms=1000,  # 等待1秒填充批次
                    compression_type='gzip',  # 更高压缩率（需Broker支持）
                    max_request_size=4194304,  # 允许4MB/请求
                    buffer_memory=134217728,  # 128MB缓冲区
                    # 可靠性配置
                    acks='all',
                    retries=5,
                    request_timeout_ms=45000
                )
            except NoBrokersAvailable:
                print("等待Kafka broker可用...")
                time.sleep(5)
                retries -= 1
        raise RuntimeError("无法连接Kafka broker")

    def _clean_value(self, value):
        """类型安全转换（保留原有逻辑）"""
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

    def _delivery_report(self, err, msg):
        """发送结果回调函数"""
        if err is not None:
            self.logger.error(f"消息发送失败: {str(err)}")
        else:
            pass  # 生产环境可记录成功指标

    def produce(self, date):
        """生产事件到Kafka"""
        events = get_github_events(date, [
            "PushEvent", "ForkEvent", "PullRequestEvent",
            "IssuesEvent", "CreateEvent", "WatchEvent",
            "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
            "IssueCommentEvent"
        ])

        success_count = 0
        for event in tqdm(events, desc="Processing GitHub Events"):
            try:
                # 清洗数据
                cleaned_event = self._clean_event(dict(event))

                # 发送消息（使用事件类型作为分区键）
                self.producer.send(
                    topic=self.topic,
                    value=cleaned_event
                )
                success_count += 1
                # 定期刷新保证进度可见性
                if success_count % 1000 == 0:
                    self.producer.flush()

            except KafkaTimeoutError as e:
                self.logger.error(f"发送超时: {str(e)}")
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"处理异常: {str(e)}")
                continue

        # 最终刷新确保所有消息发送
        self.producer.flush()
        print(f"✅ 成功生产事件到 {self.topic}")

    def __del__(self):
        """确保生产者关闭"""
        if hasattr(self, 'producer'):
            self.producer.close()

def parse_yyyymmdd(s):
    return datetime.strptime(s, "%Y%m%d").date()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub 事件爬虫（Kafka 生产）")
    parser.add_argument("--start", required=True, help="起始日期，格式为YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期，格式为YYYYMMDD")
    args = parser.parse_args()

    start_date = parse_yyyymmdd(args.start)
    end_date = parse_yyyymmdd(args.end)

    producer = KafkaEventProducer(
        bootstrap_servers='127.0.0.1:9092',
        topic='github_events'
    )

    current_date = start_date
    while current_date <= end_date:
        producer.produce(current_date)
        current_date += timedelta(days=1)