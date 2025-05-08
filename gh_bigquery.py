from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account


def get_github_events(target_date, event_types):
    """
    获取指定日期各类型GitHub事件的每小时分布

    :param target_date: datetime.date对象，要查询的日期（UTC时间）
    :param event_types: list，要查询的事件类型列表
    :return: 嵌套字典结构 {事件类型: {小时: 数量}}
    """
    credentials = service_account.Credentials.from_service_account_file('keys/proven-cogency-453510-c2-184952d551bd.json')
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    formatted_date = target_date.strftime("%Y%m%d")

    # 构建参数化查询
    query = f"""
                SELECT 
                  type,
                  created_at,
                  actor.id AS actor_id,
                  actor.login AS actor_login,
                  repo.id AS repo_id,
                  repo.name AS repo_name,
                  org.id AS org_id,
                  org.login AS org_login,
                  CASE
                    WHEN type = 'PushEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.commits') AS commits
                    ))
                    WHEN type = 'ForkEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.forkee') AS forkee  -- 包含完整的fork仓库信息
                    ))
                    WHEN type = 'PullRequestEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.pull_request') AS pull_request,  -- 完整的PR信息
                      JSON_EXTRACT(payload, '$.action') AS action
                    ))
                    WHEN type IN ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent') THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.pull_request') AS pull_request
                    ))
                    WHEN type = 'IssuesEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.issue') AS issue,  -- 完整的issue信息
                      JSON_EXTRACT(payload, '$.action') AS action
                    ))
                    WHEN type = 'IssueCommentEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.issue') AS issue
                    ))
                    WHEN type = 'CreateEvent' THEN TO_JSON_STRING(STRUCT(
                      JSON_EXTRACT(payload, '$.ref_type') AS ref_type  -- 创建类型（repository/branch等）
                    ))
                    ELSE NULL
                  END AS event_payload
                FROM 
                  `githubarchive.day.{formatted_date}`
                WHERE 
                  type IN UNNEST(@event_types)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("event_types", "STRING", event_types)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

    except Exception as e:
        print(f"查询出错: {str(e)}")
        return None

    return results


# 使用示例
if __name__ == "__main__":
    events = [
        "PushEvent",
        "ForkEvent",
        "PullRequestEvent",
        "IssuesEvent",
        "CreateEvent",
        "WatchEvent",
        "PullRequestReviewCommentEvent",
        "PullRequestReviewEvent",
        "IssueCommentEvent",
    ]

    # 查询日期（UTC时间）
    query_date = datetime(2023, 1, 3).date()
    result = get_github_events(query_date, events)
