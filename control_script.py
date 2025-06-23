#!/usr/bin/env python3
"""
GitHub数据处理系统控制脚本
用于远程控制 main_producer_2.py 的暂停和恢复
"""

import socket
import json
import argparse
from datetime import datetime

# 配置
PRODUCER_PORT = 9001
HOST = '127.0.0.1'

def send_command(command):
    """向producer发送控制命令"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # 5秒超时
            s.connect((HOST, PRODUCER_PORT))
            
            # 发送命令
            message = {
                'command': command,
                'timestamp': datetime.now().isoformat()
            }
            s.send(json.dumps(message).encode('utf-8'))
            
            # 接收响应
            response = s.recv(1024).decode('utf-8')
            return json.loads(response)
            
    except Exception as e:
        return {'status': 'error', 'message': f'连接失败: {str(e)}'}

def pause_producer():
    """暂停producer"""
    print(f"正在暂停 producer (端口 {PRODUCER_PORT})...")
    result = send_command('pause')
    print(f"  producer: {result.get('status', 'unknown')}")
    return result

def resume_producer():
    """恢复producer"""
    print(f"正在恢复 producer (端口 {PRODUCER_PORT})...")
    result = send_command('resume')
    print(f"  producer: {result.get('status', 'unknown')}")
    return result

def status_producer():
    """获取producer状态"""
    print(f"检查 producer (端口 {PRODUCER_PORT})...")
    result = send_command('status')
    print(f"  producer: {result.get('status', 'unknown')}")
    return result

def show_info():
    """显示服务信息"""
    print("=== GitHub数据处理系统控制脚本 ===")
    print("控制服务:")
    print(f"  - producer (端口 {PRODUCER_PORT}): main_producer_2.py")
    print()
    print("使用示例:")
    print("  python control_script.py status")
    print("  python control_script.py pause")
    print("  python control_script.py resume")

def main():
    parser = argparse.ArgumentParser(description="GitHub数据处理系统控制脚本")
    parser.add_argument('--host', default=HOST, help=f'目标主机地址 (默认: {HOST})')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 信息命令
    subparsers.add_parser('info', help='显示服务信息')
    
    # 暂停命令
    subparsers.add_parser('pause', help='暂停producer')
    
    # 恢复命令
    subparsers.add_parser('resume', help='恢复producer')
    
    # 状态命令
    subparsers.add_parser('status', help='获取producer状态')
    
    args = parser.parse_args()
    
    if not args.command:
        print("GitHub数据处理系统控制脚本")
        print(f"控制服务: producer (端口 {PRODUCER_PORT})")
        print()
        parser.print_help()
        return
    
    # 更新HOST（如果指定了）
    global HOST
    HOST = args.host
    
    if args.command == 'info':
        show_info()
    elif args.command == 'pause':
        pause_producer()
    elif args.command == 'resume':
        resume_producer()
    elif args.command == 'status':
        status_producer()

if __name__ == "__main__":
    main() 