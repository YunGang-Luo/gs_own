from kafka.admin import KafkaAdminClient, NewTopic

# Kafka配置
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='my_admin'
)

# 创建Topic
topic_list = [NewTopic(name="github_events", num_partitions=12, replication_factor=1), NewTopic(name="github_users", num_partitions=12, replication_factor=1), NewTopic(name="github_repos", num_partitions=12, replication_factor=1)]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topic 创建成功")
except Exception as e:
    print("创建失败:", e)
