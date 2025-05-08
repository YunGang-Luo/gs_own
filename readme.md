1. 从bigquery中读取数据： python main_producer.py --start 20250101 --end 20250131
2. 处理events（持续运行） python main_comsumer.py --workers 4
3. 从github api 中读取数据： python github_process.py --workers 2

tokens.json里面存放github的token列表
需要docker