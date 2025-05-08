import time
import threading
import requests

class TokenManager:
    def __init__(self, tokens):
        self.tokens = tokens
        self.index = 0
        self.lock = threading.Lock()
        self.token_status = {token: 0 for token in tokens}  # token -> reset_time

    def mark_token_limited(self, token):
        """仅在被403限制时调用，查一次准确的rate_limit"""
        headers = {"Authorization": f"token {token}"}
        try:
            response = requests.get("https://api.github.com/rate_limit", headers=headers)
            if response.status_code == 200:
                core = response.json()["resources"]["core"]
                reset_time = core["reset"]
                remaining = core["remaining"]
                with self.lock:
                    self.token_status[token] = reset_time
                print(f"[TokenManager] {token[:10]}... 被限制，将在 {time.strftime('%H:%M:%S', time.localtime(reset_time))} 后恢复")
            else:
                with self.lock:
                    self.token_status[token] = int(time.time()) + 60  # 默认等1分钟
                print(f"[TokenManager] 获取 rate_limit 失败，设置默认60秒等待")
        except Exception as e:
            with self.lock:
                self.token_status[token] = int(time.time()) + 60
            print(f"[TokenManager] 查询 rate_limit 异常: {e}，默认等60秒")

    def get_headers(self):
        with self.lock:
            now = int(time.time())
            for _ in range(len(self.tokens)):
                token = self.tokens[self.index]
                reset_time = self.token_status[token]
                if now >= reset_time:
                    headers = {"Authorization": f"token {token}"}
                    self.index = (self.index + 1) % len(self.tokens)
                    return headers, token
                self.index = (self.index + 1) % len(self.tokens)

        # 所有token都被限制，等待最短恢复时间
        soonest = min(self.token_status.values())
        wait_time = max(0, soonest - int(time.time()))
        print(f"[TokenManager] 所有Token受限，等待 {wait_time} 秒...")
        time.sleep(wait_time + 5)
        return self.get_headers()