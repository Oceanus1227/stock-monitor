import requests
import time
import logging

class THSAuthManager:
    """
    同花顺 iFinD Token 管理器
    实现 access_token 的自动获取、缓存与过期刷新
    """
    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        self.access_token = None
        self.expires_at = 0  # 记录 token 过期的时间戳
        
        # 同花顺获取 access_token 的官方接口
        self.auth_url = "https://quantapi.10jqka.com.cn/api/v1/get_access_token"
        
    def _fetch_new_token(self) -> str:
        """底层方法：向同花顺服务器请求新的 access_token"""
        # 根据官方文档，refresh_token 需要放在 Headers 中传递
        headers = {
            "refresh_token": self.refresh_token
        }
        
        try:
            response = requests.post(self.auth_url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # errorcode 为 0 表示成功
            if data.get("errorcode") == 0:
                self.access_token = data["data"]["access_token"]
                
                # 官方规定 access_token 有效期为 24 小时
                # 我们提前 10 分钟 (600秒) 让它过期，确保请求时绝对安全
                self.expires_at = time.time() + (24 * 3600) - 600 
                
                logging.info("✅ 成功获取新的同花顺 access_token")
                return self.access_token
            else:
                error_msg = data.get('errmsg', '未知错误')
                raise RuntimeError(f"获取 Token 失败: {error_msg}")
                
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"网络请求异常: {e}")

    def get_token(self) -> str:
        """
        对外暴露的方法：获取有效的 access_token
        如果当前 token 有效则直接返回缓存，如果过期则自动刷新
        """
        current_time = time.time()
        
        # 如果 token 存在，且当前时间还没到过期时间，直接返回缓存的 token
        if self.access_token and current_time < self.expires_at:
            return self.access_token
            
        # 否则，重新去服务器拉取
        logging.info("⏳ access_token 不存在或已过期，正在重新获取...")
        return self._fetch_new_token()
