# from elasticsearch import Elasticsearch
#
# # 连接 Elasticsearch 服务器
# es = Elasticsearch("http://127.0.0.1:9200")  # 默认本地 ES 端口是 9200
# # es.indices.refresh(index="github_user")
# # 检查 Elasticsearch 是否可用
# if not es.ping():
#     print("Elasticsearch 连接失败！")
# else:
#     print("Elasticsearch 连接成功！")
#
# # index_info = es.indices.get(index="*")  # 必须使用 `index=` 关键字参数
# #
# # for index, details in index_info.items():
# #     print(f"\n索引名: {index}")
# #     print(f"设置: {details['settings']}")
#
# # 查询 `github_user` 索引中的 `godber`
# query = {
#     "query": {
#         "match_all": {
#         }
#     },
#     "size": 1000
# }
#
# # 发送查询请求
# response = es.search(index="github_user", body=query)
#
# # 打印查询结果
# print("查询结果：")
# for hit in response["hits"]["hits"]:
#     print(hit["_source"])
#
# response = es.search(
#     index="github_repo",
#     body={"query": {"match_all": {}}},
#     track_total_hits=True  # 确保统计超过 10,000 的精确数量
# )
# print("索引中的文档总数:", response['hits']['total']['value'])


# import json
# from datetime import datetime
# from gh_bigquery import get_github_events
#
#
# a = get_github_events(datetime(2025, 1, 1).date(), ["IssueCommentEvent"])
# for i in a:
#     i = dict(i)
#     print(i)


# 统计文件有多少行
# with open("2024-01-01-15.json", "r", encoding='utf-8') as file:
#     lines = file.readlines()
#     print(f"文件总行数: {len(lines)}")


# date = datetime(2025, 1, 3).date()
# events = [
#     "PushEvent",
#     "ForkEvent",
#     "PullRequestEvent",
#     "IssuesEvent",
#     # "CreateEvent",
#     "WatchEvent",
#     "PullRequestReviewCommentEvent",
#     "PullRequestReviewEvent",
#     "IssueCommentEvent",
# ]
# res = get_github_events(date, events)
# for i in res:
#     print(i)
#     print(i.keys())
#     i = dict(i)
#     print(i)
#     print(type(i["repo_id"]))
#     print(type(i["created_at"]))
#     print(type(i["event_payload"]))
#     print(json.loads(i["event_payload"]))
#
# a = {"a": 1, "b": 2}
# a.pop("a")
# print(a)

from elasticsearch import Elasticsearch
import json

es = Elasticsearch("http://127.0.0.1:9200")

index_name = "github_repo"  # 或 github_repo

mapping = es.indices.get_mapping(index=index_name)
print(mapping)
