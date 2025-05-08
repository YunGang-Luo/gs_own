import requests
import time
from typing import Dict, Optional
import json
from token_manager import TokenManager

# ====================== 工具函数 ======================
def safe_request(url: str, token_manager: TokenManager):
    """安全的API请求，自动处理分页和错误"""
    try:
        all_data = []
        page_count = 0
        max_pages = 5
        while url and page_count < max_pages:
            headers, token = token_manager.get_headers()
            response = requests.get(url, headers=headers)

            if response.status_code not in [200, 404]:
                print(f"[safe_request] Token 受限: {token[:10]}...，正在查限速时间")
                token_manager.mark_token_limited(token)
                continue

            if response.status_code != 200:
                print(f"请求失败: {url} | 状态码: {response.status_code}")
                return None

            data = response.json()
            if isinstance(data, list):
                all_data.extend(data)
            else:
                return data

            url = response.links.get("next", {}).get("url")
            page_count += 1

        return all_data
    except Exception as e:
        print(f"请求异常: {url} | 错误: {str(e)}")
        return None

# def get_rate_limit():
#     """获取GitHub API的请求限制"""
#     response = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
#     if response.status_code == 200:
#         rate_data = response.json()
#         core = rate_data["resources"]["core"]
#         remaining = core["remaining"]
#         limit = core["limit"]
#         reset_time = core["reset"]  # 时间戳格式
#         print(f"剩余请求次数: {remaining}/{limit}")
#         print(f"限制重置时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(reset_time))} (UTC 时间戳)")
#         return remaining, reset_time
#     else:
#         print(f"请求失败，状态码: {response.status_code}")

# ====================== github_repo 数据获取 ======================
def get_repo_data(owner: str, repo: str, token_manager) -> Dict:
    """获取仓库完整数据"""
    data = {}
    
    # 1. 获取基础信息
    base_info = safe_request(f"https://api.github.com/repos/{owner}/{repo}", token_manager)
    if not base_info:
        return None

    data.update({
        "id": base_info["id"],
        "name": base_info["name"],
        "star": base_info["stargazers_count"],
        "fork": base_info["forks_count"],
        "license": base_info.get("license", {}).get("key") if base_info.get("license") else None,
        "github_organization": (base_info["owner"]["login"], base_info["owner"]["id"]) if base_info.get("owner", {}).get("type") == "Organization" else None,
        "language": base_info.get("language"),
        "topics": base_info.get("topics", []),
    })
    
    
    # 2. 获取 Issues 和 PR 统计（修正括号和条件逻辑）
    # issues = safe_request(f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&filter=all", token_manager)
    # opened_issues
    # prs = safe_request(f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all", token_manager)
    opened_issue = closed_issue = opened_pr = merged_pr = 0
    # for issue in issues:
    #     opened_issue += 1 if issue.get("state") == "open" else 0
    #     closed_issue += 1 if issue.get("state") == "closed" else 0
    # for pr in prs:
    #     opened_pr += 1 if pr.get("state") == "open" else 0
    #     merged_pr += 1 if pr.get("merged_at") is not None else 0
    data.update({
        "opened_issue": opened_issue,
        "closed_issue": closed_issue,
        "opened_pr": opened_pr,
        "merged_pr": merged_pr
    })
        
    # comments = safe_request(f"https://api.github.com/repos/{owner}/{repo}/issues/comments", token_manager)
    
    # data["comments"] = len(comments) if comments else 0
    data["comments"] = 0
    # 3. 获取代码变更数据
    # commits = safe_request(f"https://api.github.com/repos/{owner}/{repo}/commits", token_manager)
    additions = deletions = changed_files = 0
    # if commits:
    #     for commit_summary in commits[:5]:# 限制为前5个提交
    #         # 获取单个提交的详细信息
    #         commit_url = commit_summary["url"]
    #         commit_detail = safe_request(commit_url, token_manager)
    #         if not commit_detail:
    #             continue
    #
    #         # 从提交详情中提取 stats
    #         stats = commit_detail.get("stats", {})
    #         additions += stats.get("additions", 0)
    #         deletions += stats.get("deletions", 0)
    #         changed_files += stats.get("total", 0)
            
            
    
    data.update({
        "code_additions": additions,
        "code_deletions": deletions,
        "code_changed_files": changed_files,
        # "commits": len(commits) if commits else 0
        "commits": 0
    })
    
    # 4. 获取 Starred 用户列表
    starred_users = []
    stargazers_url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
    stargazers = safe_request(stargazers_url, token_manager)
    if stargazers:
        for user in stargazers:
            starred_users.append((user["login"], user["id"]))  # 添加用户的登录名
    
    data["starred_users"] = starred_users
    
    return data

# ====================== github_user 数据获取 ======================
def get_user_data(username: str, token_manager) -> Dict:
    """获取用户完整数据"""
    user_info = safe_request(f"https://api.github.com/users/{username}", token_manager)
    if not user_info:
        return None
    
    # 从 location 中提取国家（示例逻辑）
    location = user_info.get("location", "")
    country = location.split(",")[-1].strip() if location else ""
    
    return {
        "id": user_info["id"],
        "name": user_info.get("name", ""),
        "company": user_info.get("company", ""),
        "country": country
    }

# ====================== 主程序 ======================
if __name__ == "__main__":
    # 示例：获取仓库数据
    # repo_owner = "keuin"
    # repo_name = "CyberSecurityLabs"
    # repo_name = "KBackup-Fabric"
    repo_owner = "wireshark"
    repo_name = "wireshark"
    repo_data = get_repo_data(repo_owner, repo_name)
    print("仓库数据:")
    print(repo_data)
    
    # 示例：获取用户数据
    username = "DotLYHiyou"
    user_data = get_user_data(username)
    print("\n用户数据:")
    print(user_data)
    
    # 写入 json
    with open("repo_data.json", "w") as f:
        json.dump(repo_data, f, indent=4)
    with open("user_data.json", "w") as f:
        json.dump(user_data, f, indent=4)
    
    