#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通达信 .day 文件数据API服务

功能：
1. 每天下午16:10自动从 https://data.tdx.com.cn/vipdoc/hsjday.zip 下载日线数据
2. 解压到当前目录（目录结构：hsjday/{bj,sh,sz}/lday/）
3. 基于 .day 文件提供与 TDX Quant API 兼容的 Web 接口

.day 文件二进制格式（每条记录 32 字节）：
    偏移  类型    长度  说明
    0     int32   4     日期 (YYYYMMDD)
    4     int32   4     开盘价 (价格 × 100)
    8     int32   4     最高价 (价格 × 100)
    12    int32   4     最低价 (价格 × 100)
    16    int32   4     收盘价 (价格 × 100)
    20    float32 4     成交额 (元)
    24    int32   4     成交量 (股)
    28    int32   4     保留字段 (成交笔数/换手率等)

参考文档：https://help.tdx.com.cn/quant/docs/markdown/mindoc-1cfsjkbf8f3is/

启动方式：
    python tdx_api_server.py                       # 默认: waitress + 4线程 + 端口8080
    python tdx_api_server.py --port 9090           # 指定端口
    python tdx_api_server.py --threads 8           # 8个工作线程
    python tdx_api_server.py --server flask        # 使用Flask开发服务器(单线程)
    python tdx_api_server.py --help                # 查看帮助

并发模型：
    - 默认使用 Waitress 生产级 WSGI 服务器，支持多线程并发
    - 每个请求在独立线程中处理，lru_cache 线程安全 (CPython GIL)
    - 下载任务使用独立锁，防止并发下载冲突
"""

import os
import sys
import struct
import zipfile
import argparse
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union
from functools import lru_cache

import schedule
from flask import Flask, jsonify, request

# ============================================================
# 配置
# ============================================================

# 数据下载与存储
ZIP_URLS = [
    "https://data.tdx.com.cn/vipdoc/hsjday.zip",
    "https://www.tdx.com.cn/vipdoc/hsjday.zip",
    "http://data.tdx.com.cn/vipdoc/hsjday.zip",
]
DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__)))  # 当前脚本所在目录
UNZIP_DIR = DATA_DIR  # 解压后的根目录 (zip 内直接为 bj/sh/sz/lday/)
DOWNLOAD_RETRY = 2  # 每个URL的下载重试次数
DOWNLOAD_TIMEOUT = 120  # 下载超时（秒）
DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "text/html,application/zip,application/octet-stream,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.tdx.com.cn/",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

# .day 文件头大小
DAY_FILE_HEADER_SIZE = 0  # 高版本通达信 day 文件无头部，直接是 32 字节记录

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tdx_api")

# Flask 应用
app = Flask(__name__)

# 下载互斥锁：防止定时任务和手动触发同时下载
_download_lock = threading.Lock()


# ============================================================
# .day 文件解析模块
# ============================================================

# .day 文件每条记录的结构体格式 (32 bytes)
# 注意：不同版本通达信价格精度可能不同，默认按 ×100 处理
DAY_RECORD_FMT = "<IIIIIfII"  # 小端序: 4×int32 + 1×float32 + 2×int32
DAY_RECORD_SIZE = struct.calcsize(DAY_RECORD_FMT)  # 32 bytes


def parse_day_record(data: bytes) -> Optional[Dict]:
    """解析单条 32 字节的日线记录，返回字典或 None"""
    if len(data) != DAY_RECORD_SIZE:
        return None
    try:
        date_int, open_p, high_p, low_p, close_p, amount, volume, reserved = struct.unpack(
            DAY_RECORD_FMT, data
        )
        # 日期有效性检查
        if date_int < 19901219 or date_int > 20991231:
            return None
        return {
            "date": date_int,  # YYYYMMDD
            "open": open_p / 100.0,
            "high": high_p / 100.0,
            "low": low_p / 100.0,
            "close": close_p / 100.0,
            "amount": round(amount, 2),
            "volume": volume,
            "reserved": reserved,  # 沪市为成交笔数，深市部分为换手率
        }
    except struct.error:
        return None


def read_day_file(filepath: Path) -> List[Dict]:
    """
    读取整个 .day 文件，返回K线记录列表（按日期升序）。

    参数:
        filepath: .day 文件路径

    返回:
        K线数据列表，每条包含 date/open/high/low/close/amount/volume
    """
    if not filepath.exists():
        return []

    records = []
    try:
        with open(filepath, "rb") as f:
            # 跳过可能的文件头（部分旧版 day 文件可能有头）
            raw = f.read()
    except Exception as e:
        logger.error(f"读取文件失败 {filepath}: {e}")
        return []

    # 计算有效记录数：文件大小可能不是 32 的整数倍，对齐处理
    total_size = len(raw)
    record_count = total_size // DAY_RECORD_SIZE

    if total_size % DAY_RECORD_SIZE != 0:
        # 可能存在文件头或尾部多余数据
        logger.debug(f"文件 {filepath.name} 大小 {total_size} 不是32的整数倍，尝试对齐")
        # 尝试跳过前导字节
        for offset in range(min(32, total_size)):
            test_count = (total_size - offset) // DAY_RECORD_SIZE
            if (total_size - offset) % DAY_RECORD_SIZE == 0 and test_count > record_count:
                record_count = test_count
                raw = raw[offset:]
                break

    for i in range(record_count):
        record = parse_day_record(raw[i * DAY_RECORD_SIZE : (i + 1) * DAY_RECORD_SIZE])
        if record:
            records.append(record)

    return records


def get_code_from_filename(filename: str) -> str:
    """从文件名提取证券代码，如 sh000001.day -> 000001"""
    name = filename.replace(".day", "")
    # 去掉市场前缀 (sh, sz, bj)
    for prefix in ("sh", "sz", "bj"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def get_market_from_code(code: str) -> str:
    """根据证券代码判断所属市场"""
    code = code.zfill(6)
    if code.startswith(("60", "68")):
        return "sh"
    elif code.startswith(("8", "4")):  # 8xxxxx 和 4xxxxx (新三板/北交所)
        return "bj"
    else:
        return "sz"


def get_market_from_prefix(prefix: str) -> str:
    """市场前缀 -> 文件名前缀"""
    mapping = {
        "sh": "sh", "SH": "sh", "ShangHai": "sh", "上海": "sh",
        "sz": "sz", "SZ": "sz", "ShenZhen": "sz", "深圳": "sz",
        "bj": "bj", "BJ": "bj", "BeiJing": "bj", "北京": "bj",
    }
    return mapping.get(prefix, "sh")


def find_day_file(code: str, market: Optional[str] = None) -> Optional[Path]:
    """
    根据证券代码查找对应的 .day 文件。

    参数:
        code: 证券代码，如 000001, 600000, 000001
        market: 市场代码 (sh/sz/bj)，留空则自动判断

    返回:
        .day 文件的 Path，找不到返回 None
    """
    # 确定搜索的市场列表
    if market:
        markets_to_try = [get_market_from_prefix(market)]
        # 如果显式指定了market但可能不对，追加自动检测的市场
        auto_market = get_market_from_code(code)
        if auto_market not in markets_to_try:
            markets_to_try.append(auto_market)
    else:
        markets_to_try = [get_market_from_code(code)]

    for mkt in markets_to_try:
        filename = f"{mkt}{code}.day"
        path = UNZIP_DIR / mkt / "lday" / filename
        if path.exists():
            return path

    return None


@lru_cache(maxsize=8)
def list_all_codes(market: Optional[str] = None) -> List[Dict]:
    """
    列出所有可用证券代码。

    参数:
        market: 市场过滤 (sh/sz/bj)，留空返回全部

    返回:
        [{"code": "...", "market": "...", "file": "..."}, ...]
    """
    codes = []
    markets_to_search = (
        [get_market_from_prefix(market)] if market else ["sh", "sz", "bj"]
    )

    for mkt in markets_to_search:
        lday_dir = UNZIP_DIR / mkt / "lday"
        if not lday_dir.exists():
            continue
        for filepath in sorted(lday_dir.glob("*.day")):
            code = get_code_from_filename(filepath.name)
            try:
                rel_path = str(filepath.relative_to(DATA_DIR))
            except ValueError:
                rel_path = str(filepath)
            codes.append({
                "code": code,
                "market": mkt,
                "file": rel_path,
            })

    return codes


# ============================================================
# K线聚合模块（日线 -> 周线/月线）
# ============================================================

def aggregate_to_weekly(daily_records: List[Dict]) -> List[Dict]:
    """将日线数据聚合为周线数据"""
    if not daily_records:
        return []

    weekly = []
    current_week_data = []
    current_week = None

    for record in daily_records:
        date_str = str(record["date"])
        dt = datetime.strptime(date_str, "%Y%m%d")
        week_key = dt.strftime("%Y%W")  # 年+周数

        if week_key != current_week:
            if current_week_data:
                weekly.append(_merge_records_to_bar(current_week_data))
            current_week = week_key
            current_week_data = [record]
        else:
            current_week_data.append(record)

    if current_week_data:
        weekly.append(_merge_records_to_bar(current_week_data))

    return weekly


def aggregate_to_monthly(daily_records: List[Dict]) -> List[Dict]:
    """将日线数据聚合为月线数据"""
    if not daily_records:
        return []

    monthly = []
    current_month_data = []
    current_month = None

    for record in daily_records:
        date_str = str(record["date"])
        month_key = date_str[:-2]  # YYYYMM

        if month_key != current_month:
            if current_month_data:
                monthly.append(_merge_records_to_bar(current_month_data))
            current_month = month_key
            current_month_data = [record]
        else:
            current_month_data.append(record)

    if current_month_data:
        monthly.append(_merge_records_to_bar(current_month_data))

    return monthly


def _merge_records_to_bar(records: List[Dict]) -> Dict:
    """将多条日线记录合并为一根K线"""
    if not records:
        return {}
    first = records[0]
    last = records[-1]
    return {
        "date": first["date"],
        "open": first["open"],
        "high": max(r["high"] for r in records),
        "low": min(r["low"] for r in records),
        "close": last["close"],
        "amount": sum(r["amount"] for r in records),
        "volume": sum(r["volume"] for r in records),
    }


# ============================================================
# 复权计算模块
# ============================================================

def calculate_adjust_factor(daily_records: List[Dict]) -> List[Dict]:
    """
    基于简单算法计算前复权因子。
    注意：此方法基于涨跌幅估算，非精确除权数据。精确复权需要除权除息数据文件。
    """
    if len(daily_records) < 2:
        return daily_records

    factor = 1.0
    adjusted = []

    # 从新到旧遍历
    for i in range(len(daily_records) - 1, -1, -1):
        record = daily_records[i].copy()
        if i < len(daily_records) - 1:
            prev = daily_records[i + 1]
            # 如果有跳空缺口（可能除权），估算调整因子
            if prev["close"] > 0 and record["close"] > 0:
                expected_change = record["close"] / record["open"] if record["open"] > 0 else 1.0
                natural_next_open = prev["close"] * expected_change
                diff_ratio = prev["open"] / natural_next_open if natural_next_open > 0 else 1.0
                # 如果偏离超过 10%，认为是除权
                if abs(diff_ratio - 1.0) > 0.10:
                    factor *= diff_ratio

        for key in ("open", "high", "low", "close"):
            record[key] = round(record[key] * factor, 3)
        adjusted.append(record)

    adjusted.reverse()
    return adjusted


# ============================================================
# 数据下载模块
# ============================================================

def _is_valid_zip(filepath: Path) -> bool:
    """检查文件是否为有效的 zip 文件 (PK magic bytes)"""
    try:
        with open(filepath, "rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False


def _try_download_url(url: str, zip_path: Path, session) -> bool:
    """尝试从单个 URL 下载，返回是否成功"""
    for attempt in range(DOWNLOAD_RETRY):
        try:
            logger.info(f"  下载尝试 {attempt + 1}/{DOWNLOAD_RETRY}: {url}")
            resp = session.get(url, timeout=DOWNLOAD_TIMEOUT, headers=DOWNLOAD_HEADERS)

            if resp.status_code != 200:
                logger.warning(f"    HTTP {resp.status_code}, 跳过")
                continue

            content = resp.content

            # 检测是否返回了 HTML 页面而非 zip 文件
            if b"<!DOCTYPE" in content[:200] or b"<html" in content[:200].lower() or b"<script" in content[:200].lower():
                logger.warning(f"    返回了 HTML/JS 页面 (反爬虫拦截), 大小: {len(content)} 字节")
                continue

            if len(content) < 1024:
                logger.warning(f"    文件太小 ({len(content)} 字节), 可能不是有效数据")
                continue

            with open(zip_path, "wb") as f:
                f.write(content)

            if _is_valid_zip(zip_path):
                logger.info(f"    ✓ 下载成功: {len(content)} 字节")
                return True
            else:
                logger.warning(f"    文件不是有效的 zip (无 PK 签名)")
                continue

        except Exception as e:
            logger.warning(f"    下载异常: {e}")
            if attempt < DOWNLOAD_RETRY - 1:
                time.sleep(5)

    return False


def download_and_extract() -> bool:
    """
    下载 hsjday.zip 并解压到当前目录（线程安全：使用互斥锁防止并发下载）。

    下载策略:
        1. 优先检查本地是否已有 hsjday.zip，有则直接解压（不重复下载）
        2. 依次尝试多个镜像 URL
        3. 验证下载内容是否为有效 zip
        4. 解压后验证目录结构

    返回:
        True 表示成功，False 表示失败
    """
    if not _download_lock.acquire(blocking=False):
        logger.warning("下载任务已在执行中，跳过本次请求")
        return False

    try:
        zip_path = DATA_DIR / "hsjday.zip"

        # ---- 步骤1: 检查本地 zip 是否今天下载的 ----
        today_str = datetime.now().strftime("%Y%m%d")
        skip_download = False
        if zip_path.exists() and _is_valid_zip(zip_path):
            mtime = datetime.fromtimestamp(zip_path.stat().st_mtime)
            if mtime.strftime("%Y%m%d") == today_str:
                logger.info(f"本地 zip 今天已下载 ({zip_path.stat().st_size} 字节), 跳过下载直接解压")
                skip_download = True
            else:
                logger.info(f"本地 zip 过期 (下载于 {mtime.strftime('%Y-%m-%d %H:%M')}), 删除并重新下载")
                zip_path.unlink()

        if skip_download:
            pass  # 使用已有 zip
        else:
            # ---- 步骤2: 从网络下载 ----
            logger.info(f"开始下载数据, 共 {len(ZIP_URLS)} 个备用 URL")
            try:
                import requests
                session = requests.Session()
                session.headers.update(DOWNLOAD_HEADERS)
            except ImportError:
                logger.error("需要 requests 库: pip install requests")
                return False

            downloaded = False
            for url in ZIP_URLS:
                if _try_download_url(url, zip_path, session):
                    downloaded = True
                    break

            if not downloaded:
                logger.error(
                    "所有下载 URL 均失败。可能原因:\n"
                    "  1. 网络环境无法访问 data.tdx.com.cn\n"
                    "  2. 被反爬虫系统拦截 (EO_Bot_Ssid)\n"
                    "  3. 解决方法: 手动下载 hsjday.zip 放到脚本目录下，重启服务即可自动解压"
                )
                return False

        # ---- 步骤3: 解压 ----
        if not zip_path.exists():
            logger.error("zip 文件不存在")
            return False

        if not _is_valid_zip(zip_path):
            logger.error("zip 文件无效")
            return False

        try:
            logger.info(f"解压到: {DATA_DIR}")
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(DATA_DIR)
            logger.info("解压完成")

            # 清理 zip 文件（确保下次下载新数据，不会被本地缓存跳过）
            zip_path.unlink(missing_ok=True)
            logger.info("已删除临时 zip 文件")

            # ---- 步骤4: 验证目录结构 ----
            has_data = False
            for mkt in ("sh", "sz", "bj"):
                lday = UNZIP_DIR / mkt / "lday"
                if lday.exists():
                    file_count = len(list(lday.glob("*.day")))
                    logger.info(f"  市场 {mkt}: {file_count} 个 .day 文件")
                    has_data = True
                else:
                    logger.warning(f"  市场 {mkt}: lday 目录不存在")

            if not has_data:
                logger.error("解压后未找到任何 .day 文件，目录结构可能不正确")
                return False

            # 清除所有文件缓存
            _clear_all_caches()

            return True
        except zipfile.BadZipFile as e:
            logger.error(f"解压失败 (文件损坏): {e}")
            # 删除损坏的 zip 以便下次重新下载
            zip_path.unlink(missing_ok=True)
            return False
        except Exception as e:
            logger.error(f"解压失败: {e}")
            return False
    finally:
        _download_lock.release()


def scheduled_task():
    """定时任务：每天下载并解压数据"""
    logger.info("=== 定时下载任务开始 ===")
    success = download_and_extract()
    if success:
        logger.info("=== 定时下载任务成功完成 ===")
    else:
        logger.error("=== 定时下载任务失败 ===")


def run_scheduler():
    """在独立线程中运行定时任务调度器"""
    # 每天 16:10 执行
    schedule.every().day.at("16:10").do(scheduled_task)

    # 启动时先执行一次（如果数据目录为空）
    if not UNZIP_DIR.exists() or not list(UNZIP_DIR.rglob("*.day")):
        logger.info("首次启动，立即下载数据...")
        scheduled_task()

    logger.info(f"定时任务已设置: 每天 16:10 下载 hsjday.zip ({len(ZIP_URLS)} 个备用URL)")
    while True:
        schedule.run_pending()
        time.sleep(30)  # 每 30 秒检查一次


# ============================================================
# 状态缓存 (避免频繁扫描文件系统)
# ============================================================

_status_cache = None
_status_cache_time = 0
_status_cache_ttl = 60  # 状态缓存有效期（秒）
_status_cache_lock = threading.Lock()


def get_data_status_cached() -> Dict:
    """带 TTL 的状态查询，避免每次请求都扫描 12000+ 文件"""
    global _status_cache, _status_cache_time
    now = time.time()
    with _status_cache_lock:
        if _status_cache is not None and (now - _status_cache_time) < _status_cache_ttl:
            return _status_cache

    # 重建缓存
    status = {}
    total_files = 0
    date_range = [None, None]

    for mkt in ("sh", "sz", "bj"):
        lday = UNZIP_DIR / mkt / "lday"
        if lday.exists():
            files = list(lday.glob("*.day"))
            file_count = len(files)
            total_files += file_count
            status[mkt] = {"file_count": file_count, "sample_codes": []}

            for f in list(files)[:3]:
                records = read_day_file(f)
                code = get_code_from_filename(f.name)
                if records:
                    first_date = records[0]["date"]
                    last_date = records[-1]["date"]
                    status[mkt]["sample_codes"].append({
                        "code": code,
                        "records": len(records),
                        "first_date": str(first_date),
                        "last_date": str(last_date),
                    })
                    if date_range[0] is None or first_date < date_range[0]:
                        date_range[0] = first_date
                    if date_range[1] is None or last_date > date_range[1]:
                        date_range[1] = last_date
        else:
            status[mkt] = {"error": "数据目录不存在"}

    result = {
        "data_dir": str(DATA_DIR),
        "unzip_dir": str(UNZIP_DIR),
        "total_files": total_files,
        "date_range": {
            "start": str(date_range[0]) if date_range[0] else None,
            "end": str(date_range[1]) if date_range[1] else None,
        },
        "markets": status,
        "cached_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    with _status_cache_lock:
        _status_cache = result
        _status_cache_time = now

    return result


# ============================================================
# 缓存 (线程安全)
# ============================================================

# 每文件读取锁：确保同一文件不会被多线程同时读取
_file_locks = {}
_file_locks_lock = threading.Lock()

# 手动二级缓存：在 lru_cache 之外额外保护，防止缓存未命中时的重复读取
_read_through_cache = {}
_read_through_lock = threading.Lock()


@lru_cache(maxsize=1024)
def get_bars_cache(code: str, market: str) -> List[Dict]:
    """
    缓存读取的K线数据（线程安全）。

    使用 lru_cache + 手动 read-through 缓存双重保护。
    多线程同时请求未缓存的同一code时，只有一个线程读文件。
    """
    filepath = find_day_file(code, market)
    if filepath is None:
        return []

    cache_key = f"{code}:{market}"
    file_key = str(filepath)

    # 快速路径：检查手动缓存（避免 lru_cache 内部竞争窗口）
    with _read_through_lock:
        if cache_key in _read_through_cache:
            return _read_through_cache[cache_key]

    # 慢速路径：获取文件锁，读取文件
    with _file_locks_lock:
        if file_key not in _file_locks:
            _file_locks[file_key] = threading.Lock()

    with _file_locks[file_key]:
        # 双重检查：等锁期间可能已被其他线程填充
        with _read_through_lock:
            if cache_key in _read_through_cache:
                return _read_through_cache[cache_key]

        result = read_day_file(filepath)

        # 填充到手动缓存（同时 lru_cache 也会缓存返回值）
        with _read_through_lock:
            _read_through_cache[cache_key] = result

        return result


def _clear_all_caches():
    """清除所有缓存（下载完成后调用）"""
    global _status_cache, _status_cache_time
    get_bars_cache.cache_clear()
    list_all_codes.cache_clear()
    with _read_through_lock:
        _read_through_cache.clear()
    with _file_locks_lock:
        _file_locks.clear()
    with _status_cache_lock:
        _status_cache = None
        _status_cache_time = 0


# ============================================================
# API 接口 (与 TDX Quant API 兼容)
# ============================================================

def _build_bar_response(records: List[Dict]) -> List[Dict]:
    """将内部记录格式转换为 API 响应格式"""
    result = []
    for r in records:
        result.append({
            "date": str(r["date"]),  # YYYYMMDD 字符串
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "amount": r["amount"],
            "volume": r["volume"],
        })
    return result


def _parse_period(period: Union[str, int]) -> str:
    """解析周期参数"""
    period_map = {
        "0": "daily", "day": "daily", "daily": "daily", "日线": "daily",
        "1": "weekly", "week": "weekly", "weekly": "weekly", "周线": "weekly",
        "2": "monthly", "month": "monthly", "monthly": "monthly", "月线": "monthly",
    }
    if isinstance(period, int):
        period = str(period)
    return period_map.get(period.lower(), "daily")


# ---- 核心 API ----


@app.route("/api/GetSecurityBars", methods=["GET", "POST"])
def api_get_security_bars():
    """
    获取证券K线数据 (兼容 TDX Quant API)

    参数:
        category:  市场品种 (0:沪深, 1:期货, 留空默认0)
        market:    市场代码 (sh/sz/bj, 留空自动判断)
        code:      证券代码 (如 000001, 600000)
        count:     返回K线数量 (默认全部，最大 10000)
        period:    周期 (0:日线, 1:周线, 2:月线)
        fqtype:    复权类型 (0:不复权, 1:前复权)
        start:     起始日期 (YYYYMMDD, 可选)
        end:       结束日期 (YYYYMMDD, 可选)

    返回格式:
        {
            "code": "000001",
            "market": "sz",
            "count": 100,
            "bars": [{"date": "20240607", "open": ..., "high": ..., ...}]
        }
    """
    code = _get_param("code")
    if not code:
        return _error_response("缺少参数: code")

    market = _get_param("market", "")
    if not market:
        market = get_market_from_code(code)
    else:
        market = get_market_from_prefix(market)

    code = code.strip().zfill(6)
    period = _parse_period(_get_param("period", "0"))
    fqtype = int(_get_param("fqtype", "0"))
    count = int(_get_param("count", "0"))  # 0 表示全部
    start_date = _get_param("start", "")
    end_date = _get_param("end", "")

    if count <= 0:
        count = 10000  # 最大返回数量

    # 读取日线数据
    records = get_bars_cache(code, market)
    if not records:
        return _error_response(f"未找到数据: code={code}, market={market}")

    # 日期过滤
    if start_date:
        try:
            start_int = int(start_date)
            records = [r for r in records if r["date"] >= start_int]
        except ValueError:
            return _error_response(f"日期格式错误: {start_date}")

    if end_date:
        try:
            end_int = int(end_date)
            records = [r for r in records if r["date"] <= end_int]
        except ValueError:
            return _error_response(f"日期格式错误: {end_date}")

    # 复权处理
    if fqtype == 1:
        records = calculate_adjust_factor(records)
    # fqtype == 2 (后复权) 暂不支持

    # 周期聚合
    if period == "weekly":
        records = aggregate_to_weekly(records)
    elif period == "monthly":
        records = aggregate_to_monthly(records)

    # 数量限制（取最新的 count 条）
    if len(records) > count:
        records = records[-count:]

    return jsonify({
        "code": code,
        "market": market,
        "period": period,
        "fqtype": fqtype,
        "count": len(records),
        "bars": _build_bar_response(records),
    })


@app.route("/api/GetIndexBars", methods=["GET", "POST"])
def api_get_index_bars():
    """
    获取指数K线数据 (兼容 TDX Quant API)

    与 GetSecurityBars 类似，但默认查找指数代码。沪深指数代码如:
        000001 上证指数, 399001 深证成指, 399006 创业板指
    """
    code = _get_param("code")
    if not code:
        return _error_response("缺少参数: code")

    market = _get_param("market", "")
    if not market:
        market = get_market_from_code(code)
    else:
        market = get_market_from_prefix(market)

    code = code.strip().zfill(6)
    period = _parse_period(_get_param("period", "0"))
    count = int(_get_param("count", "0"))
    start_date = _get_param("start", "")
    end_date = _get_param("end", "")

    if count <= 0:
        count = 10000

    records = get_bars_cache(code, market)
    if not records:
        return _error_response(f"未找到指数数据: code={code}")

    if start_date:
        try:
            start_int = int(start_date)
            records = [r for r in records if r["date"] >= start_int]
        except ValueError:
            return _error_response(f"日期格式错误: {start_date}")

    if end_date:
        try:
            end_int = int(end_date)
            records = [r for r in records if r["date"] <= end_int]
        except ValueError:
            return _error_response(f"日期格式错误: {end_date}")

    if period == "weekly":
        records = aggregate_to_weekly(records)
    elif period == "monthly":
        records = aggregate_to_monthly(records)

    if len(records) > count:
        records = records[-count:]

    return jsonify({
        "code": code,
        "market": market,
        "period": period,
        "count": len(records),
        "bars": _build_bar_response(records),
    })


@app.route("/api/GetSecurityList", methods=["GET", "POST"])
def api_get_security_list():
    """
    获取证券列表

    参数:
        market: 市场代码 (sh/sz/bj), 留空返回全部
        category: 市场品种 (0:沪深, 默认0)

    返回:
        {"list": [{"code": "...", "market": "...", "file": "..."}, ...]}
    """
    market = _get_param("market", "")
    if market:
        market = get_market_from_prefix(market)
    else:
        market = None

    codes = list_all_codes(market)
    return jsonify({
        "market": market or "all",
        "count": len(codes),
        "list": codes,
    })


@app.route("/api/GetSecurityQuotes", methods=["GET", "POST"])
def api_get_security_quotes():
    """
    获取证券最新行情（取 .day 文件最后一条记录）

    参数:
        codes: 证券代码列表，逗号分隔 (如 "000001,600000,000002")
        market: 市场代码，多个代码时自动判断

    返回:
        {"quotes": [{"code": "...", "date": ..., "open": ..., ...}]}
    """
    codes_str = _get_param("codes", "")
    if not codes_str:
        return _error_response("缺少参数: codes")

    codes = [c.strip().zfill(6) for c in codes_str.split(",") if c.strip()]
    if not codes:
        return _error_response("证券代码列表为空")

    quotes = []
    for code in codes:
        market = get_market_from_code(code)
        records = get_bars_cache(code, market)
        if records:
            latest = records[-1].copy()
            latest["code"] = code
            latest["market"] = market
            quotes.append(latest)
        else:
            quotes.append({"code": code, "market": market, "error": "未找到数据"})

    return jsonify({"quotes": quotes})


@app.route("/api/GetFinanceInfo", methods=["GET", "POST"])
def api_get_finance_info():
    """
    获取财务信息

    注意：.day 文件不包含财务数据，此接口返回提示信息。
    如需完整财务数据功能，请参考 https://help.tdx.com.cn/quant/
    """
    return jsonify({
        "message": "当前 .day 文件数据源不包含财务信息。该接口预留用于扩展数据源。",
        "hint": "如需完整财务数据，请使用通达信量化平台 SDK。",
    })


@app.route("/api/GetKLine", methods=["GET", "POST"])
def api_get_kline():
    """
    获取K线数据（扩展接口，支持批量代码查询）

    参数:
        codes:    证券代码列表，逗号分隔
        period:   周期 (day/week/month)
        count:    返回数量 (默认 100)
        fqtype:   复权类型 (0:不复权, 1:前复权)
    """
    codes_str = _get_param("codes", "")
    if not codes_str:
        return _error_response("缺少参数: codes")

    codes = [c.strip().zfill(6) for c in codes_str.split(",") if c.strip()]
    period = _parse_period(_get_param("period", "day"))
    count = int(_get_param("count", "100"))
    fqtype = int(_get_param("fqtype", "0"))

    if count <= 0:
        count = 10000

    result = {}
    for code in codes:
        market = get_market_from_code(code)
        records = get_bars_cache(code, market)

        if not records:
            result[code] = {"error": "未找到数据"}
            continue

        if fqtype == 1:
            records = calculate_adjust_factor(records)

        if period == "weekly":
            records = aggregate_to_weekly(records)
        elif period == "monthly":
            records = aggregate_to_monthly(records)

        if len(records) > count:
            records = records[-count:]

        result[code] = {
            "market": market,
            "period": period,
            "count": len(records),
            "bars": _build_bar_response(records),
        }

    return jsonify(result)


# ---- 工具 API ----


@app.route("/api/GetDataStatus", methods=["GET", "POST"])
def api_get_data_status():
    """获取数据状态信息（带60秒缓存，避免频繁扫描文件系统）"""
    result = get_data_status_cached()
    return jsonify(result)


@app.route("/api/DownloadNow", methods=["POST"])
def api_download_now():
    """手动触发数据下载"""
    thread = threading.Thread(target=scheduled_task, daemon=True)
    thread.start()
    return jsonify({"message": "数据下载已在后台启动，请通过 /api/GetDataStatus 查看进度"})


@app.route("/api/health", methods=["GET"])
def api_health():
    """健康检查"""
    return jsonify({
        "status": "ok",
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_available": UNZIP_DIR.exists(),
    })


# ---- 辅助函数 ----


def _get_param(key: str, default: str = "") -> str:
    """从 GET 或 POST 参数中获取值"""
    if request.method == "POST" and request.is_json:
        return str(request.json.get(key, default))
    return request.args.get(key, request.form.get(key, default))


def _error_response(message: str, status_code: int = 400):
    """返回标准错误响应"""
    return jsonify({"error": message, "status": status_code}), status_code


# ---- 错误处理 ----


@app.errorhandler(404)
def handle_404(e):
    return jsonify({
        "error": "接口不存在",
        "available_apis": [
            "/api/GetSecurityBars",
            "/api/GetIndexBars",
            "/api/GetSecurityList",
            "/api/GetSecurityQuotes",
            "/api/GetFinanceInfo",
            "/api/GetKLine",
            "/api/GetDataStatus",
            "/api/DownloadNow",
            "/api/health",
        ],
    }), 404


@app.errorhandler(500)
def handle_500(e):
    return jsonify({"error": "服务器内部错误", "detail": str(e)}), 500


# ---- 首页 ----


@app.route("/")
def index():
    """API 文档首页"""
    return jsonify({
        "service": "TDX Day File API Server",
        "version": "1.0.0",
        "description": "基于通达信 .day 文件的行情数据API服务",
        "reference": "https://help.tdx.com.cn/quant/docs/markdown/mindoc-1cfsjkbf8f3is/",
        "endpoints": {
            "/api/GetSecurityBars": "获取证券K线数据 (GET/POST)",
            "/api/GetIndexBars": "获取指数K线数据 (GET/POST)",
            "/api/GetSecurityList": "获取证券列表 (GET/POST)",
            "/api/GetSecurityQuotes": "获取最新行情 (GET/POST)",
            "/api/GetFinanceInfo": "财务信息接口 (预留) (GET/POST)",
            "/api/GetKLine": "批量K线查询 (GET/POST)",
            "/api/GetDataStatus": "数据状态查询 (GET/POST)",
            "/api/DownloadNow": "手动触发下载 (POST)",
            "/api/health": "健康检查 (GET)",
        },
        "usage_examples": {
            "GetSecurityBars": "/api/GetSecurityBars?code=000001&market=sz&period=0&count=10",
            "GetIndexBars": "/api/GetIndexBars?code=000001&market=sh&period=0&count=10",
            "GetSecurityList": "/api/GetSecurityList?market=sh",
            "GetSecurityQuotes": "/api/GetSecurityQuotes?codes=000001,600000,000002",
            "GetKLine": "/api/GetKLine?codes=000001,600000&period=week&count=20&fqtype=1",
        },
    })


# ============================================================
# 主程序入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="通达信 .day 文件数据 API 服务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python tdx_api_server.py                        # 默认: waitress + 4线程 + 端口8080
  python tdx_api_server.py --port 9090            # 指定端口
  python tdx_api_server.py --threads 8            # 8个工作线程
  python tdx_api_server.py --server flask         # Flask开发服务器
  python tdx_api_server.py --no-scheduler         # 仅启动API，不自动下载
  python tdx_api_server.py --download-only        # 仅下载数据，不启动API
        """,
    )
    parser.add_argument("--port", type=int, default=8080, help="API 服务端口 (默认: 8080)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="绑定地址 (默认: 0.0.0.0)")
    parser.add_argument("--server", type=str, default="waitress",
                        choices=["waitress", "flask"],
                        help="WSGI 服务器 (默认: waitress, 可选: flask)")
    parser.add_argument("--threads", type=int, default=4,
                        help="Waitress 工作线程数 (默认: 4, 范围: 1-64)")
    parser.add_argument("--no-scheduler", action="store_true", help="禁用定时下载任务")
    parser.add_argument("--download-only", action="store_true", help="仅下载数据后退出")
    parser.add_argument("--debug", action="store_true", help="启用调试模式")
    args = parser.parse_args()

    # 校验线程数
    threads = max(1, min(args.threads, 64))

    print("=" * 60)
    print("  通达信 .day 文件数据 API 服务 v1.1.0")
    print("  兼容 TDX Quant API 接口规范")
    print("=" * 60)

    # 仅下载模式
    if args.download_only:
        success = download_and_extract()
        sys.exit(0 if success else 1)

    # 启动定时下载线程
    if not args.no_scheduler:
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info(f"定时下载线程已启动 (每天 16:10)")
    else:
        logger.info("定时下载已禁用 (--no-scheduler)")

    # 启动 WSGI 服务
    print(f"\n  服务器: {args.server.upper()}")
    print(f"  并发模型: {'多线程 (' + str(threads) + ' workers)' if args.server == 'waitress' else '单线程 (开发模式)'}")
    print(f"  线程安全: lru_cache (GIL) + 下载互斥锁")
    print(f"\n  API 服务地址: http://{args.host}:{args.port}")
    print(f"  API 文档首页: http://localhost:{args.port}/")
    print(f"  健康检查:     http://localhost:{args.port}/api/health")
    print(f"  数据状态:     http://localhost:{args.port}/api/GetDataStatus")
    print(f"\n  按 Ctrl+C 停止服务\n")

    if args.server == "waitress":
        try:
            from waitress import serve
            logger.info(f"Waitress 启动: host={args.host}, port={args.port}, threads={threads}")
            serve(
                app,
                host=args.host,
                port=args.port,
                threads=threads,
                connection_limit=512,          # 最大并发连接数
                channel_timeout=120,           # 请求超时（秒）
                cleanup_interval=30,           # 清理间隔
            )
        except ImportError:
            logger.warning("waitress 未安装，回退到 Flask 开发服务器。")
            logger.warning("安装命令: pip install waitress")
            app.run(
                host=args.host,
                port=args.port,
                debug=args.debug,
                threaded=True,                 # Flask 多线程模式
                use_reloader=False,
            )
    else:
        # Flask 开发服务器 (调试/开发用)
        logger.info(f"Flask 开发服务器启动: host={args.host}, port={args.port}, threaded=True")
        app.run(
            host=args.host,
            port=args.port,
            debug=args.debug,
            threaded=True,                     # 启用多线程
            use_reloader=False,                # 避免 scheduler 线程重复启动
        )


if __name__ == "__main__":
    main()
