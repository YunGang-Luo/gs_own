import argparse
import threading
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from gh_bigquery import get_github_events
import logging
from control_server import create_control_server, CONTROL_PORTS
import os


# 全局暂停标志
is_paused = False
pause_lock = threading.Lock()

# 状态文件路径
CHECKPOINT_FILE = 'producer_checkpoint.json'


class IncrementalProducer:
    """增量爬取生产者"""
    
    def __init__(self, topic='github_events', bootstrap_servers='127.0.0.1:9092'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._create_producer()
        self.last_successful_date = self._load_checkpoint()

        # 日志配置
        logging.basicConfig(
            filename='producer_errors.log',
            level=logging.ERROR,
            format='%(asctime)s %(name)s ERROR: %(message)s'
        )
        self.logger = logging.getLogger('IncrementalProducer')

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

    def _load_checkpoint(self):
        """加载最后成功爬取的日期"""
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    last_date = data.get('last_successful_date')
                    if last_date:
                        last_date = datetime.strptime(last_date, '%Y-%m-%d').date()
                        print(f"加载检查点: 最后成功日期 {last_date}")
                        return last_date
                    else:
                        print("未找到最后成功日期")
                        return None
            else:
                print("未找到检查点文件")
                return None
        except Exception as e:
            print(f"加载检查点失败: {e}")
            return None

    def _save_checkpoint(self, date):
        """保存最后成功爬取的日期"""
        try:
            save_data = {
                'last_successful_date': date.isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)
            
            print(f"检查点已保存: {date}")
        except Exception as e:
            self.logger.error(f"保存检查点失败: {e}")

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
        date_str = date.isoformat()
        print(f"开始处理日期: {date_str}")
        
        try:
            events = get_github_events(date, [
                "PushEvent", "ForkEvent", "PullRequestEvent",
                "IssuesEvent", "CreateEvent", "WatchEvent",
                "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
                "IssueCommentEvent"
            ])

            success_count = 0
            for event in tqdm(events, desc=f"Processing {date_str}"):
                # 检查暂停状态
                with pause_lock:
                    if is_paused:
                        time.sleep(1)  # 暂停时等待1秒
                        continue

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
            print(f"成功生产 {success_count} 个事件到 {self.topic}")
            
            # 更新最后成功日期
            self.last_successful_date = date
            self._save_checkpoint(date)
            return True
            
        except Exception as e:
            self.logger.error(f"处理日期 {date_str} 失败: {e}")
            return False

    def get_incremental_start_date(self, user_start_date):
        """获取增量爬取的起始日期"""
        if self.last_successful_date:
            # 从最后成功日期的下一天开始
            incremental_start = self.last_successful_date + timedelta(days=1)
            if incremental_start > user_start_date:
                print(f"增量爬取: 从 {incremental_start} 开始")
                return incremental_start
            else:
                print(f"全量爬取: 从用户指定日期 {user_start_date} 开始")
                return user_start_date
        else:
            print(f"首次爬取: 从用户指定日期 {user_start_date} 开始")
            return user_start_date

    def get_status(self):
        """获取爬取状态"""
        return {
            'last_successful_date': self.last_successful_date,
            'last_updated': datetime.now().isoformat() if self.last_successful_date else None
        }

    def __del__(self):
        """确保生产者关闭"""
        if hasattr(self, 'producer'):
            self.producer.close()


def parse_yyyymmdd(s):
    return datetime.strptime(s, "%Y%m%d").date()


def pause_all():
    """暂停所有生产者进程"""
    global is_paused
    with pause_lock:
        is_paused = True
    print("所有生产者进程已暂停")

def resume_all():
    """恢复所有生产者进程"""
    global is_paused
    with pause_lock:
        is_paused = False
    print("所有生产者进程已恢复")

def get_status():
    """获取当前状态"""
    with pause_lock:
        return 'paused' if is_paused else 'running'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GitHub 事件增量爬虫（Kafka 生产）")
    parser.add_argument("--start", required=True, help="起始日期，格式为YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期，格式为YYYYMMDD")
    parser.add_argument("--force", action="store_true", help="强制从指定起始日期开始（忽略检查点）")
    parser.add_argument("--status", action="store_true", help="显示爬取状态后退出")
    args = parser.parse_args()

    start_date = parse_yyyymmdd(args.start)
    end_date = parse_yyyymmdd(args.end)

    # 启动控制服务器
    control_server = create_control_server(
        port=CONTROL_PORTS['producer'],
        pause_func=pause_all,
        resume_func=resume_all,
        status_func=get_status
    )

    producer = IncrementalProducer(
        bootstrap_servers='127.0.0.1:9092',
        topic='github_events'
    )

    # 显示状态
    if args.status:
        status = producer.get_status()
        print("\n=== 爬取状态 ===")
        print(f"最后成功日期: {status['last_successful_date']}")
        print(f"最后更新时间: {status['last_updated']}")
        exit(0)

    # 获取增量起始日期
    if args.force:
        actual_start_date = start_date
        print(f"强制模式: 从指定日期 {start_date} 开始")
    else:
        actual_start_date = producer.get_incremental_start_date(start_date)
    
    if actual_start_date > end_date:
        print("所有指定日期范围的数据都已处理完成")
        exit(0)

    print(f"开始爬取: {actual_start_date} 到 {end_date}")

    current_date = actual_start_date
    while current_date <= end_date:
        success = producer.produce(current_date)
        if not success:
            print(f"处理日期 {current_date} 失败，继续下一个日期")
        current_date += timedelta(days=1)
    
    print("爬取完成！") 