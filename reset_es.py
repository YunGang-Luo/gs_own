from elasticsearch import Elasticsearch, helpers, NotFoundError

es = Elasticsearch("http://127.0.0.1:9200")

USER_INDEX = "github_user"
REPO_INDEX = "github_repo"

def backup_index_data(index_name):
    """备份索引数据为列表"""
    try:
        result = helpers.scan(es, index=index_name)
        return [doc['_source'] for doc in result]
    except NotFoundError:
        print(f"{index_name} 索引不存在，跳过备份。")
        return []

def delete_index(index_name):
    try:
        es.indices.delete(index=index_name)
        print(f"已删除索引 {index_name}")
    except NotFoundError:
        print(f"索引 {index_name} 不存在，跳过删除")

def create_user_index():
    """使用原始结构重建 github_user 索引"""
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "long"},
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "flag": {"type": "long"}
            }
        }
    }
    es.indices.create(index=USER_INDEX, body=mapping)
    print("已创建 github_user 索引")

def create_repo_index():
    """使用原始结构重建 github_repo 索引"""
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "long"},
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "star": {"type": "long"},
                "flag": {"type": "long"}
            }
        }
    }
    es.indices.create(index=REPO_INDEX, body=mapping)
    print("已创建 github_repo 索引")

def restore_user_data(users):
    for user in users:
        restored = {
            "id": user.get("id"),
            "name": user.get("name"),
            "flag": -1
        }
        es.index(index=USER_INDEX, id=restored["id"], document=restored)
    print(f"已恢复 {len(users)} 条用户数据")

def restore_repo_data(repos):
    for repo in repos:
        restored = {
            "id": repo.get("id"),
            "name": repo.get("name"),
            "star": 0,
            "flag": -1
        }
        es.index(index=REPO_INDEX, id=restored["id"], document=restored)
    print(f"已恢复 {len(repos)} 条仓库数据")

if __name__ == "__main__":
    if not es.ping():
        print("无法连接 Elasticsearch")
        exit(1)

    old_users = backup_index_data(USER_INDEX)
    old_repos = backup_index_data(REPO_INDEX)

    delete_index(USER_INDEX)
    delete_index(REPO_INDEX)

    create_user_index()
    create_repo_index()

    restore_user_data(old_users)
    restore_repo_data(old_repos)

    print("索引重建与数据恢复完成")
