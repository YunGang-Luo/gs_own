from TuGraphClient import TuGraphClient
from datetime import datetime, timezone


client = TuGraphClient("127.0.0.1:7070", "admin", "73@TuGraph")


def iso_to_int32(dt) -> int:
    """将 datetime 对象转换为 int32 时间戳"""
    # 确保时区为 UTC
    if isinstance(dt, int):
        # 如果是整数，直接返回
        return dt
    if isinstance(dt, str):
        # 如果是字符串，尝试解析为 UTC 时间
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("无效的 ISO 格式时间字符串")
    if dt.tzinfo is None:
        # 若无时区信息，强制设为 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # 若有时区信息，转换为 UTC
        dt = dt.astimezone(timezone.utc)

    timestamp = int(dt.timestamp())

    # 校验 int32 范围
    if not (-2147483648 <= timestamp <= 2147483647):
        return 0
    return timestamp


def format_property_value(value):
    """格式化属性值为Cypher语法字符串"""
    if isinstance(value, str):
        # 分步处理转义：先替换单引号，再包裹
        escaped_value = value.replace("'", r"\'")  # 使用原始字符串避免转义问题
        return f"'{escaped_value}'"
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif value is None:
        return "null"
    else:  # 数字类型直接返回
        return str(value)


def create_node(label: str, properties: dict):
    """动态插入节点（直接拼接参数）"""
    try:
        # 生成属性键值对字符串
        props_str = ", ".join(
            [f"{k}: {format_property_value(v)}" for k, v in properties.items()]
        )
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN n"

        res = client.call_cypher(cypher)
        # print(f"插入 {label} 节点成功，返回值: {res}")
        return res
    except Exception as e:
        print(f"插入失败: {str(e)}")
        print(properties)
        return None


def update_node(label: str, match_props: dict, set_props: dict):
    """动态更新节点（直接拼接参数）"""
    try:
        # 生成匹配条件
        match_conditions = ", ".join(
            [f"{k}: {format_property_value(v)}" for k, v in match_props.items()]
        )

        # 生成SET语句
        set_statements = ", ".join(
            [f"n.{k} = {format_property_value(v)}" for k, v in set_props.items()]
        )

        cypher = f"""
        MATCH (n:{label} {{{match_conditions}}})
        SET {set_statements}
        RETURN n
        """

        res = client.call_cypher(cypher)
        # print(f"更新 {label} 节点成功，响应: {res}")
        return res
    except Exception as e:
        print(f"更新失败: {str(e)}")
        print(match_props)
        print(set_props)
        return None


def create_relationship(
        start_node: dict,  # 格式: {"label": "Person", "properties": {"id": 1}}
        end_node: dict,  # 格式: {"label": "Person", "properties": {"id": 2}}
        rel_type: str,  # 关系类型，如 "KNOWS"
        rel_props: dict  # 关系属性
):
    """创建带属性的边"""
    try:
        # 生成起点匹配条件
        start_props = ", ".join(
            [f"{k}: {format_property_value(v)}"
             for k, v in start_node['properties'].items()]
        )
        # 生成终点匹配条件
        end_props = ", ".join(
            [f"{k}: {format_property_value(v)}"
             for k, v in end_node['properties'].items()]
        )
        # 生成边属性
        rel_props_str = ", ".join(
            [f"{k}: {format_property_value(v)}"
             for k, v in rel_props.items()]
        ) if rel_props else ""

        cypher = f"""
        MATCH (a:{start_node['label']} {{{start_props}}}),
              (b:{end_node['label']} {{{end_props}}})
        CREATE (a)-[r:{rel_type} {{{rel_props_str}}}]->(b)
        RETURN r
        """

        res = client.call_cypher(cypher)
        # print(f"创建 {rel_type} 关系成功，返回值: {res}")
        return res
    except Exception as e:
        print(f"创建关系失败: {str(e)}")
        print(cypher)
        return None


# 使用示例
if __name__ == "__main__":
    # 动态插入数据
    # create_node("issue", {
    #     # "name": "guoguo",
    #     "id": "123454",
    #     # "country": "China"  # 支持任意新属性
    # })

    # 动态更新数据
    # update_node("github_user",
    #             match_props={"name": "guoguo"},  # 匹配条件
    #             set_props={"country": "USA"}  # 更新内容
    #             )

    create_relationship(
        start_node={
            "label": "github_user",
            "properties": {"id": 123454}
        },
        end_node={
            "label": "issue",
            "properties": {"id": "123454"}
        },
        rel_type="open_issue",
        rel_props={
            "created_at": 123456789,
        }
    )