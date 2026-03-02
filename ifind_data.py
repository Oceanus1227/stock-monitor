"""
iFinD HTTP API 数据获取模块

从 akshare 迁移到 iFinD HTTP API，保持接口兼容
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class iFinDError(Exception):
    """iFinD API 错误"""
    pass


class iFinDDataClient:
    """iFinD HTTP API 客户端"""
    
    BASE_URL = "https://quantapi.51ifind.com/api/v1"
    
    def __init__(self, refresh_token: Optional[str] = None, auto_refresh: bool = True):
        self.refresh_token = refresh_token or os.getenv("IFIND_REFRESH_TOKEN")
        if not self.refresh_token:
            raise iFinDError("请提供 refresh_token 或设置环境变量 IFIND_REFRESH_TOKEN")
        
        self.auto_refresh = auto_refresh
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        
    def _get_access_token(self) -> str:
        """获取当前有效的access_token"""
        if (self._access_token is None or 
            self._token_expires_at is None or 
            datetime.now() > self._token_expires_at - timedelta(hours=1)):
            self._refresh_token()
        return self._access_token
    
    def _refresh_token(self) -> None:
        """用refresh_token换取新的access_token"""
        url = f"{self.BASE_URL}/get_access_token"
        params = {"refresh_token": self.refresh_token}
        
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != 0:
                raise iFinDError(f"获取access_token失败: {data.get('msg', '未知错误')}")
            
            self._access_token = data["data"]["access_token"]
            self._token_expires_at = datetime.now() + timedelta(days=7)
            
        except requests.RequestException as e:
            raise iFinDError(f"请求access_token失败: {e}")
    
    def call_api(self, func: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """调用iFinD API"""
        access_token = self._get_access_token()
        
        url = f"{self.BASE_URL}/{func}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            resp = requests.post(url, json=params, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            # token过期自动重试
            if data.get("code") in [-340, -403] or "token" in str(data.get("msg", "")).lower():
                if self.auto_refresh:
                    self._refresh_token()
                    return self.call_api(func, params)
            
            if data.get("code") != 0:
                raise iFinDError(f"API错误: {data.get('msg', '未知错误')} (code: {data.get('code')})")
            
            return data
            
        except requests.RequestException as e:
            raise iFinDError(f"请求失败: {e}")
    
    def get_dp(self, code: str, indicators: str = "close,open,high,low,volume",
               start_date: Optional[str] = None, end_date: Optional[str] = None,
               period: str = "D") -> pd.DataFrame:
        """获取日线数据 (THS_DP 接口)"""
        params = {
            "code": code,
            "indicator": indicators,
            "period": period
        }
        if start_date:
            params["startdate"] = start_date.replace("-", "")
        if end_date:
            params["enddate"] = end_date.replace("-", "")
        
        result = self.call_api("THS_DP", params)
        
        if "tables" not in result or not result["tables"]:
            raise iFinDError(f"无数据返回: {code}")
        
        table = result["tables"][0]
        df = pd.DataFrame(table)
        
        # 标准化列名
        column_map = {
            "TIME": "date",
            "THS_CLOSE": "close",
            "THS_OPEN": "open",
            "THS_HIGH": "high",
            "THS_LOW": "low",
            "THS_VOLUME": "volume"
        }
        df = df.rename(columns=column_map)
        
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df.set_index("date", inplace=True)
        
        # 确保数值类型
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df


# 全局客户端实例（延迟初始化）
_ifind_client: Optional[iFinDDataClient] = None

def get_ifind_client() -> iFinDDataClient:
    """获取全局 iFinD 客户端实例"""
    global _ifind_client
    if _ifind_client is None:
        _ifind_client = iFinDDataClient()
    return _ifind_client


def _format_stock_code(code: str) -> str:
    """
    转换股票代码格式，添加交易所后缀
    600XXX -> 600XXX.SH
    000XXX/300XXX -> XXX.SZ
    """
    code = str(code).strip()
    
    if "." in code:
        return code
    
    if code.startswith("6") or code.startswith("688") or code.startswith("689"):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"
