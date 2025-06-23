#!/usr/bin/env python3
"""
共享控制服务器库
提供统一的网络控制接口，用于远程控制GitHub数据处理系统的producer组件
"""

import socket
import json
import threading
import time
from datetime import datetime


class ControlServer:
    """通用控制服务器类"""
    
    def __init__(self, port, pause_func, resume_func, status_func=None):
        """
        初始化控制服务器
        
        Args:
            port: 监听端口
            pause_func: 暂停函数
            resume_func: 恢复函数
            status_func: 状态查询函数（可选）
        """
        self.port = port
        self.pause_func = pause_func
        self.resume_func = resume_func
        self.status_func = status_func
        self.server_socket = None
        self.is_running = True
        
    def start(self):
        """启动控制服务器（后台线程）"""
        def server_thread():
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(1)  # 1秒超时，用于检查停止信号
            
            print(f"控制服务器已启动，监听端口 {self.port}")
            
            while self.is_running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    # 为每个客户端连接创建新线程
                    client_thread = threading.Thread(
                        target=self.handle_client, 
                        args=(client_socket, addr),
                        daemon=True
                    )
                    client_thread.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.is_running:
                        print(f"控制服务器错误: {e}")
                    break
                    
            if self.server_socket:
                self.server_socket.close()
        
        # 启动后台线程
        thread = threading.Thread(target=server_thread, daemon=True)
        thread.start()
        return thread
        
    def handle_client(self, client_socket, addr):
        """处理客户端连接"""
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                return
                
            message = json.loads(data)
            command = message.get('command')
            
            response = {'status': 'unknown'}
            
            if command == 'pause':
                self.pause_func()
                response = {'status': 'success', 'message': '已暂停'}
                print(f"收到暂停命令 (端口 {self.port})")
                
            elif command == 'resume':
                self.resume_func()
                response = {'status': 'success', 'message': '已恢复'}
                print(f"收到恢复命令 (端口 {self.port})")
                
            elif command == 'status':
                if self.status_func:
                    status = self.status_func()
                else:
                    status = 'unknown'
                response = {'status': 'success', 'state': status}
                
            else:
                response = {'status': 'error', 'message': '未知命令'}
                
            # 发送响应
            response_data = json.dumps(response).encode('utf-8')
            client_socket.send(response_data)
            
        except Exception as e:
            try:
                error_response = {'status': 'error', 'message': str(e)}
                client_socket.send(json.dumps(error_response).encode('utf-8'))
            except:
                pass
        finally:
            try:
                client_socket.close()
            except:
                pass
            
    def stop(self):
        """停止控制服务器"""
        self.is_running = False
        if self.server_socket:
            self.server_socket.close()


def create_control_server(port, pause_func, resume_func, status_func=None):
    """
    创建并启动控制服务器的便捷函数
    
    Args:
        port: 监听端口
        pause_func: 暂停函数
        resume_func: 恢复函数
        status_func: 状态查询函数（可选）
    
    Returns:
        ControlServer实例
    """
    server = ControlServer(port, pause_func, resume_func, status_func)
    server.start()
    return server


# 预定义的端口配置
CONTROL_PORTS = {
    'producer': 9001,    # main_producer_2.py 控制端口
    # 'consumer': 9002,  # main_consumer_2.py 控制端口 - 已移除
    # 'processor': 9003  # github_process_2.py 控制端口 - 已移除
} 