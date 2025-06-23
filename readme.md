1. 从bigquery中读取数据： python main_producer.py --start 20250101 --end 20250131
2. 处理events（持续运行） python main_comsumer.py --workers 4
3. 从github api 中读取数据： python github_process.py --workers 2

tokens.json里面存放github的token列表
需要docker

## 改动

1. **main_producer_2.py**: 在main_producer.py的基础上增加暂停/恢复功能和增量爬取功能
   - 运行指令：`python main_producer_2.py --start 20250101 --end 20250131`

2. **tu_util_2.py**: 在tu_util.py上的基础上做修改，不再对tugraph做操作，而是返回cypher语句

3. **main_consumer_2.py**: 在main_consumer.py的基础上做修改，不再对tugraph做操作，而是返回cypher语句
   - 运行指令：`python main_consumer_2.py --workers 4`

4. **github_process_2.py**: 在github_process.py的基础上做修改，不再对tugraph做操作，而是向kafka的"cypher"主题传输cypher语句
   - 运行指令：`python github_process_2.py --workers 2`

5. **cypher_to_mysql.py**: 向mysql的"cypher"表导入cypher语句，主键"id"顺序增长
   - 运行指令：`python cypher_to_mysql.py --batch-size 100 --batch-timeout 5`

6. **create_cypher_table.sql**: 在mysql中创建"cypher"表的sql语句

7. **control_script.py**: 远程控制main_producer_2.py的暂停与恢复
   - 运行指令：
     - 查看状态：`python control_script.py status`
     - 暂停服务：`python control_script.py pause`
     - 恢复服务：`python control_script.py resume`
     - 显示信息：`python control_script.py info`