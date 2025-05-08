from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError, NotFoundError

es = Elasticsearch("http://127.0.0.1:9200")

if not es.ping():
    print("Elasticsearch 连接失败！")
else:
    print("Elasticsearch 连接成功！")

# ----------------- 查询接口 -----------------
def get_user_by_id(user_id):
    """根据用户ID查询用户信息"""
    try:
        response = es.get(index="github_user", id=user_id)
        return response['_source']
    except NotFoundError:
        print(f"用户ID {user_id} 不存在")
        return None
    except Exception as e:
        print(f"查询用户时发生错误: {str(e)}")
        return None

def get_repo_by_id(repo_id):
    """根据仓库ID查询仓库信息"""
    try:
        response = es.get(index="github_repo", id=repo_id)
        return response['_source']
    except NotFoundError:
        print(f"仓库ID {repo_id} 不存在")
        return None
    except Exception as e:
        print(f"查询仓库时发生错误: {str(e)}")
        return None

# ----------------- 原有创建接口 -----------------
def create_user(new_user):
    try:
        es.index(index="github_user", id=new_user['id'], document=new_user)
        return True
    except ConflictError:
        print(f"用户ID {new_user['id']} 已存在，未插入。")
        return False

def create_repo(new_repo):
    try:
        es.index(index="github_repo", id=new_repo['id'], document=new_repo)
        return True
    except ConflictError:
        print(f"仓库ID {new_repo['id']} 已存在，未插入。")
        return False

def update_user_by_id(user_id, update_data):
    """根据用户ID更新用户信息（部分更新）"""
    try:
        es.update(
            index="github_user",
            id=user_id,
            body={"doc": update_data}
        )
        return True
    except NotFoundError:
        print(f"用户ID {user_id} 不存在，更新失败")
        return False
    except Exception as e:
        print(f"更新用户时发生错误: {str(e)}")
        return False

def update_repo_by_id(repo_id, update_data):
    """根据仓库ID更新仓库信息（部分更新）"""
    try:
        es.update(
            index="github_repo",
            id=repo_id,
            body={"doc": update_data}
        )
        return True
    except NotFoundError:
        print(f"仓库ID {repo_id} 不存在，更新失败")
        return False
    except Exception as e:
        print(f"更新仓库时发生错误: {str(e)}")
        return False

if __name__ == "__main__":
    # 测试创建用户
    a = get_repo_by_id(845589133)
    print(type(a))