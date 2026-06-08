# 说明

当前脚本 API 基于通达信官方 `.day` 日线二进制文件实现，通过每天 16:10 自动下载 `hsjday.zip` 获取沪深北三市全部证券的日线行情数据（共 12,000+ 只标的，最早可追溯至 1990 年）。

## 支持的功能

| 功能 | 说明 |
|------|------|
| 日/周/月K线 | 基于日线聚合为周线和月线 |
| 指数K线 | 上证指数、深证成指、创业板指等 |
| 证券列表 | 按市场查询所有可用代码 |
| 最新行情 | 批量获取多只证券最新日线 |
| 前复权 | 基于跳空缺口估算（非精确除权） |
| 日期过滤 | 按起始/结束日期筛选 K 线 |
| 定时下载 | 每日 16:10 自动更新数据 |
| 多线程并发 | Waitress 生产级服务器，支持 1000+ QPS |

## 不具备的功能

| 功能 | 原因 |
|------|------|
| **分钟级数据**（5m/15m/30m/1h） | `.day` 文件仅包含日线，分钟数据需 `.lc5`/`.lc1` 文件 |
| **实时行情推送** | 无 Level-2 数据源，仅支持基于日线的静态查询 |
| **精确复权** | 需 `.gbbq` 除权除息文件，`.day` 文件不含除权细节 |
| **财务数据** | 财务报表、股本信息等不在日线数据范围内 |
| **期货/港股/美股** | `hsjday.zip` 仅包含沪深北 A 股数据 |
| **交易下单** | 本服务为只读行情 API，不具备交易功能 |
| ** Tick 级数据** | 无逐笔成交数据源 |


# TDX Day File API 接口文档 v1.1.0

> 基于通达信 `.day` 二进制文件的行情数据 API 服务  
> 兼容 [TDX Quant API](https://help.tdx.com.cn/quant/docs/markdown/mindoc-1cfsjkbf8f3is/) 接口规范

---

## 目录

- [1. 服务概述](#1-服务概述)
- [2. 快速开始](#2-快速开始)
- [3. .day 文件格式](#3-day-文件格式)
- [4. 通用说明](#4-通用说明)
- [5. API 接口](#5-api-接口)
  - [5.1 GetSecurityBars — 获取证券K线](#51-getsecuritybars-获取证券k线)
  - [5.2 GetIndexBars — 获取指数K线](#52-getindexbars-获取指数k线)
  - [5.3 GetSecurityList — 获取证券列表](#53-getsecuritylist-获取证券列表)
  - [5.4 GetSecurityQuotes — 获取最新行情](#54-getsecurityquotes-获取最新行情)
  - [5.5 GetKLine — 批量K线查询](#55-getkline-批量k线查询)
  - [5.6 GetDataStatus — 数据状态](#56-getdatastatus-数据状态)
  - [5.7 GetFinanceInfo — 财务信息](#57-getfinanceinfo-财务信息)
  - [5.8 DownloadNow — 手动触发下载](#58-downloadnow-手动触发下载)
  - [5.9 health — 健康检查](#59-health-健康检查)
- [6. 市场代码规则](#6-市场代码规则)
- [7. 周期参数](#7-周期参数)
- [8. 复权说明](#8-复权说明)
- [9. 错误码](#9-错误码)
- [10. 并发能力](#10-并发能力)
- [11. 部署运维](#11-部署运维)

---

## 1. 服务概述

| 项目 | 说明 |
|------|------|
| **数据源** | `https://data.tdx.com.cn/vipdoc/hsjday.zip` |
| **数据格式** | 通达信 `.day` 二进制文件，每条 K 线 32 字节 |
| **数据覆盖** | 沪深北三市全部证券日线数据，共 12,000+ 只标的 |
| **数据起始** | 上证指数自 1990-12-19 起 |
| **更新频率** | 每日 16:10 自动下载最新数据 |
| **并发模型** | Waitress 多线程 WSGI（默认 4 workers，可配 1-64） |
| **线程安全** | GIL 保护 lru_cache + 下载互斥锁 + read-through 缓存 |
| **传输协议** | HTTP/1.1 RESTful，支持 GET 和 POST |
| **响应格式** | JSON (Content-Type: application/json) |

---

## 2. 快速开始

### 安装

```bash
pip install -r requirements.txt
```

### 启动服务

```bash
# 生产模式 (waitress 4线程, 端口 8080)
python tdx_api_server.py

# 高并发模式
python tdx_api_server.py --threads 16 --port 18080

# 开发模式
python tdx_api_server.py --server flask --debug

# 仅下载数据
python tdx_api_server.py --download-only

# 禁用自动更新
python tdx_api_server.py --no-scheduler
```

### 命令行参数

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `--port` | int | 8080 | 服务端口 |
| `--host` | str | 0.0.0.0 | 绑定地址 |
| `--server` | str | waitress | WSGI 服务器: `waitress` / `flask` |
| `--threads` | int | 4 | Waitress 工作线程数 (1-64) |
| `--no-scheduler` | flag | false | 禁用定时下载 |
| `--download-only` | flag | false | 仅下载数据后退出 |
| `--debug` | flag | false | 调试模式 |

### 验证服务

```bash
curl http://localhost:8080/api/health
# {"data_available":true,"server_time":"2026-06-08 16:10:00","status":"ok"}
```

---

## 3. .day 文件格式

每条 K 线记录固定 **32 字节**，小端序 (Little-Endian) 存储：

| 偏移 | 类型 | 字节 | 说明 | 示例 |
|------|------|------|------|------|
| 0 | `int32` | 4 | 日期 YYYYMMDD | `20260605` |
| 4 | `int32` | 4 | 开盘价 × 100 | `1050` = 10.50 元 |
| 8 | `int32` | 4 | 最高价 × 100 | `1100` = 11.00 元 |
| 12 | `int32` | 4 | 最低价 × 100 | `1030` = 10.30 元 |
| 16 | `int32` | 4 | 收盘价 × 100 | `1080` = 10.80 元 |
| 20 | `float32` | 4 | 成交额（元） | `123456789.0` |
| 24 | `int32` | 4 | 成交量（股） | `5000000` |
| 28 | `int32` | 4 | 保留字段 | — |

**Python 解析示例**：

```python
import struct

fmt = "<IIIIIfII"  # 小端序: 4×uint32 + float32 + 2×uint32
with open("sh000001.day", "rb") as f:
    raw = f.read(32)
    date, open_p, high_p, low_p, close_p, amount, volume, _ = struct.unpack(fmt, raw)
    print(f"{date}: O={open_p/100:.2f} H={high_p/100:.2f} "
          f"L={low_p/100:.2f} C={close_p/100:.2f} V={volume}")
```

**目录结构**：

```
tdx_api/
├── sh/lday/sh000001.day   ← 上证指数
├── sh/lday/sh600000.day   ← 浦发银行
├── sz/lday/sz000001.day   ← 平安银行
├── sz/lday/sz399001.day   ← 深证成指
└── bj/lday/bj830799.day   ← 北交所个股
```

---

## 4. 通用说明

### 请求方式

所有接口同时支持 **GET** 和 **POST**：

- **GET**: 参数通过 URL query string 传递
- **POST**: 参数通过 JSON body 传递 (`Content-Type: application/json`)

### 响应格式

**成功响应** (HTTP 200)：

```json
{
  "code": "000001",
  "market": "sz",
  "count": 10,
  "bars": [...]
}
```

**错误响应** (HTTP 400/404/405/500)：

```json
{
  "error": "错误描述",
  "status": 400
}
```

### K线数据对象 (Bar)

```json
{
  "date": "20260605",
  "open": 11.05,
  "high": 11.20,
  "low": 10.90,
  "close": 11.12,
  "amount": 123456789.00,
  "volume": 5000000
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | string | 日期 YYYYMMDD |
| `open` | float | 开盘价（元） |
| `high` | float | 最高价（元） |
| `low` | float | 最低价（元） |
| `close` | float | 收盘价（元） |
| `amount` | float | 成交额（元） |
| `volume` | int | 成交量（股） |

> **注意**: 响应中 **不含** `reserved` 保留字段，API 自动过滤。

---

## 5. API 接口

### 5.1 GetSecurityBars — 获取证券K线

获取指定证券的历史 K 线数据，支持日/周/月线、复权、日期过滤。

**请求**

```
GET/POST /api/GetSecurityBars
```

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `code` | string | **是** | — | 证券代码，如 `000001`、`600000`、`688981` |
| `market` | string | 否 | auto | 市场: `sh`/`sz`/`bj`，留空自动判断 |
| `count` | int | 否 | 10000 | 返回 K 线条数（取最近 N 条） |
| `period` | string/int | 否 | `0` | 周期: `0`=日线, `1`=周线, `2`=月线 |
| `fqtype` | int | 否 | `0` | 复权: `0`=不复权, `1`=前复权 |
| `start` | string | 否 | — | 起始日期 `YYYYMMDD` |
| `end` | string | 否 | — | 结束日期 `YYYYMMDD` |

**响应**

```json
{
  "code": "000001",
  "market": "sz",
  "period": "daily",
  "fqtype": 0,
  "count": 3,
  "bars": [
    {"date": "20260603", "open": 11.05, "high": 11.20, "low": 10.90, "close": 11.12, "amount": 123456789.00, "volume": 5000000},
    {"date": "20260604", "open": 11.12, "high": 11.25, "low": 10.95, "close": 11.08, "amount": 98765432.00, "volume": 4200000},
    {"date": "20260605", "open": 11.08, "high": 11.30, "low": 10.85, "close": 10.98, "amount": 110500000.00, "volume": 5800000}
  ]
}
```

**示例**

```bash
# 平安银行最近 10 条日线
curl "http://localhost:8080/api/GetSecurityBars?code=000001&market=sz&count=10"

# 浦发银行最近 20 根周线（前复权）
curl "http://localhost:8080/api/GetSecurityBars?code=600000&market=sh&period=1&fqtype=1&count=20"

# 中芯国际 2025年Q1 日线
curl "http://localhost:8080/api/GetSecurityBars?code=688981&start=20250101&end=20250331&count=200"

# 北交所个股月线
curl "http://localhost:8080/api/GetSecurityBars?code=830799&period=2&count=12"

# POST 方式
curl -X POST "http://localhost:8080/api/GetSecurityBars" \
  -H "Content-Type: application/json" \
  -d '{"code":"000001","market":"sz","period":"weekly","count":5}'
```

---

### 5.2 GetIndexBars — 获取指数K线

与 `GetSecurityBars` 参数完全相同，语义上用于指数查询。

**常用指数代码**：

| 指数 | 代码 | market |
|------|------|--------|
| 上证指数 | `000001` | `sh` |
| 深证成指 | `399001` | `sz` |
| 创业板指 | `399006` | `sz` |
| 科创50 | `000688` | `sh` |
| 沪深300 | `000300` | `sh` |
| 中证500 | `000905` | `sh` |
| 上证50 | `000016` | `sh` |
| 北证50 | `899050` | `bj` |

**示例**

```bash
# 上证指数最近 5 根周线
curl "http://localhost:8080/api/GetIndexBars?code=000001&market=sh&period=1&count=5"

# 科创板50最近30条日线
curl "http://localhost:8080/api/GetIndexBars?code=000688&market=sh&count=30"
```

---

### 5.3 GetSecurityList — 获取证券列表

列出所有可用的证券代码及所属市场。

**请求**

```
GET/POST /api/GetSecurityList
```

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `market` | string | 否 | `all` | 市场过滤: `sh`/`sz`/`bj`，留空返回全部 |

**响应**

```json
{
  "market": "all",
  "count": 12204,
  "list": [
    {"code": "000001", "market": "sh", "file": "sh/lday/sh000001.day"},
    {"code": "000002", "market": "sh", "file": "sh/lday/sh000002.day"},
    {"code": "600000", "market": "sh", "file": "sh/lday/sh600000.day"},
    ...
  ]
}
```

**示例**

```bash
# 全部证券
curl "http://localhost:8080/api/GetSecurityList"

# 仅沪市
curl "http://localhost:8080/api/GetSecurityList?market=sh"

# 仅北交所
curl "http://localhost:8080/api/GetSecurityList?market=bj"
```

---

### 5.4 GetSecurityQuotes — 获取最新行情

批量获取多只证券的最新日线数据（`.day` 文件最后一条记录）。

**请求**

```
GET/POST /api/GetSecurityQuotes
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `codes` | string | **是** | 证券代码列表，逗号分隔，如 `"000001,600000,688981"` |

**响应**

```json
{
  "quotes": [
    {"code": "000001", "market": "sz", "date": 20260605, "open": 11.08, "high": 11.30, "low": 10.85, "close": 10.98, "amount": 110500000.00, "volume": 5800000},
    {"code": "600000", "market": "sh", "date": 20260605, "open": 9.50, "high": 9.58, "low": 9.28, "close": 9.34, "amount": 280000000.00, "volume": 30000000},
    {"code": "999999", "market": "sz", "error": "未找到数据"}
  ]
}
```

> 不存在的代码会在 `quotes` 数组中返回 `error` 字段，不会导致整体请求失败。

**示例**

```bash
curl "http://localhost:8080/api/GetSecurityQuotes?codes=000001,600000,688981,830799"
```

---

### 5.5 GetKLine — 批量K线查询

一次请求获取多只证券的 K 线数据，返回以代码为 key 的对象。

**请求**

```
GET/POST /api/GetKLine
```

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `codes` | string | **是** | — | 逗号分隔的代码列表 |
| `period` | string | 否 | `day` | 周期: `day`/`week`/`month` |
| `count` | int | 否 | `100` | 每只返回的 K 线条数 |
| `fqtype` | int | 否 | `0` | 复权类型 |

**响应**

```json
{
  "000001": {
    "market": "sz",
    "period": "weekly",
    "count": 3,
    "bars": [
      {"date": "20260518", "open": 10.90, "high": 11.30, "low": 10.80, "close": 10.98, "amount": 550000000.00, "volume": 25000000},
      ...
    ]
  },
  "600000": {
    "market": "sh",
    "period": "weekly",
    "count": 3,
    "bars": [...]
  }
}
```

> 不存在的代码不返回 error，直接不出现在结果中。

**示例**

```bash
# 批量周线
curl "http://localhost:8080/api/GetKLine?codes=000001,600000,688981&period=week&count=20"

# 批量月线（前复权）
curl "http://localhost:8080/api/GetKLine?codes=000001,600000&period=month&count=12&fqtype=1"
```

---

### 5.6 GetDataStatus — 数据状态

查询当前数据覆盖范围、文件数量、日期范围。

**请求**

```
GET/POST /api/GetDataStatus
```

无参数。

**响应**（带 60 秒缓存）

```json
{
  "data_dir": "d:\\Project\\tdx_api",
  "unzip_dir": "d:\\Project\\tdx_api",
  "total_files": 12204,
  "date_range": {
    "start": "19901219",
    "end": "20260605"
  },
  "markets": {
    "sh": {
      "file_count": 5867,
      "sample_codes": [
        {"code": "000001", "records": 8655, "first_date": "19901219", "last_date": "20260605"},
        {"code": "000002", "records": 8655, "first_date": "19901219", "last_date": "20260605"}
      ]
    },
    "sz": {
      "file_count": 5756,
      "sample_codes": [...]
    },
    "bj": {
      "file_count": 581,
      "sample_codes": [...]
    }
  },
  "cached_at": "2026-06-08 16:10:00"
}
```

**示例**

```bash
curl "http://localhost:8080/api/GetDataStatus"
```

---

### 5.7 GetFinanceInfo — 财务信息

**请求**

```
GET/POST /api/GetFinanceInfo
```

**响应**

```json
{
  "message": "当前 .day 文件数据源不包含财务信息。该接口预留用于扩展数据源。",
  "hint": "如需完整财务数据，请使用通达信量化平台 SDK。"
}
```

> `.day` 文件仅包含行情数据，不含财务报表。此接口为兼容 TDX Quant API 预留。

---

### 5.8 DownloadNow — 手动触发下载

立即触发一次数据下载（异步后台执行）。

**请求**

```
POST /api/DownloadNow
```

> ⚠️ 仅支持 POST。同一时间只允许一个下载任务，并发请求会被互斥锁拒绝。

**响应**

```json
{
  "message": "数据下载已在后台启动，请通过 /api/GetDataStatus 查看进度"
}
```

**示例**

```bash
curl -X POST "http://localhost:8080/api/DownloadNow"
```

---

### 5.9 health — 健康检查

**请求**

```
GET /api/health
```

**响应**

```json
{
  "status": "ok",
  "server_time": "2026-06-08 16:10:00",
  "data_available": true
}
```

| 字段 | 说明 |
|------|------|
| `status` | `"ok"` 表示服务正常 |
| `server_time` | 服务器当前时间 |
| `data_available` | `true`=数据目录存在，`false`=尚未下载数据 |

---

## 6. 市场代码规则

| 代码前缀 | 市场 | 前缀 | 目录 |
|----------|------|------|------|
| `60xxxx`, `68xxxx` | 上海证券交易所 | `sh` | `sh/lday/shXXXXXX.day` |
| `4xxxxx`, `8xxxxx` | 北京证券交易所 | `bj` | `bj/lday/bjXXXXXX.day` |
| 其他 (`0xxxxx`, `3xxxxx`, `1xxxxx`, `2xxxxx`) | 深圳证券交易所 | `sz` | `sz/lday/szXXXXXX.day` |

> `market` 参数留空时会自动根据代码前缀判断市场。显式指定 `market` 不匹配时，API 会自动回退到正确市场。

---

## 7. 周期参数

| 参数值 | 含义 | 数据来源 |
|--------|------|----------|
| `0`, `"day"`, `"daily"`, `"日线"` | 日线 | 直接读取 `.day` 文件 |
| `1`, `"week"`, `"weekly"`, `"周线"` | 周线 | 日线按周聚合 |
| `2`, `"month"`, `"monthly"`, `"月线"` | 月线 | 日线按月聚合 |

**聚合规则**：

| K线 | open | high | low | close | amount | volume |
|-----|------|------|-----|-------|--------|--------|
| 周线 | 周一开盘 | 周内最高 | 周内最低 | 周五收盘 | 周内合计 | 周内合计 |
| 月线 | 月初开盘 | 月内最高 | 月内最低 | 月末收盘 | 月内合计 | 月内合计 |

---

## 8. 复权说明

| fqtype | 含义 | 说明 |
|--------|------|------|
| `0` | 不复权 | 原始价格，直接从 `.day` 文件读取 |
| `1` | 前复权 | 基于跳空缺口（>10%）估算调整因子 |

> ⚠️ **注意**：前复权为**估算值**，基于相邻两日价格跳空幅度计算，非精确除权数据。精确复权需要除权除息数据文件（`.gbbq` 等），不在 `.day` 文件范围内。

---

## 9. 错误码

| HTTP 状态码 | 含义 | 示例 |
|-------------|------|------|
| `200` | 成功 | — |
| `400` | 请求参数错误 | 缺少必填参数、日期格式错误、代码不存在 |
| `404` | 接口不存在 | 访问未定义的 API 路径 |
| `405` | 方法不允许 | 对仅 POST 的接口使用 GET |
| `500` | 服务器内部错误 | 文件读取异常 |

**典型错误响应**：

```json
// 缺少 code 参数
{"error": "缺少参数: code", "status": 400}

// 代码不存在
{"error": "未找到数据: code=999999, market=sz", "status": 400}

// 接口不存在
{"error": "接口不存在", "available_apis": ["/api/GetSecurityBars", ...], "status": 404}

// 日期格式错误
{"error": "日期格式错误: abc", "status": 400}
```

---

## 10. 并发能力

| 场景 | QPS | 错误率 | 延迟 (P50) |
|------|-----|--------|------------|
| 缓存命中（同代码反复查） | **~1,200** | 0% | ~2ms |
| 多股票轮询 | **~1,200** | 0% | ~5ms |
| 批量K线查询 | **~1,200** | 0% | ~5ms |
| 混合负载（含文件扫描） | **~55** | 0% | ~200ms |
| 极限吞吐（20,000 请求） | **~1,220** | 0% | ~8ms |

> 测试环境：Waitress 4 workers × 12,204 只股票 × Windows 11 × Python 3.13

**扩展建议**：

| 线程数 | 预估 QPS | 适用场景 |
|--------|---------|----------|
| 4 (默认) | ~1,200 | 单机轻量部署 |
| **8** | ~1,800-2,000 | 生产环境推荐 |
| 16 | ~2,000-2,500 | 高并发场景 |
| 32 | ~2,500-3,000 | 极限（GIL 瓶颈） |

```bash
python tdx_api_server.py --threads 8 --port 18080
```

---

## 11. 部署运维

### 数据更新

```
每日 16:10 自动下载 → 解压覆盖 → 清除缓存 → 新数据立即可用
```

手动触发：

```bash
curl -X POST "http://localhost:8080/api/DownloadNow"
```

### 手动获取数据（网络受限时）

在能访问外网的机器上：

```bash
curl -L -o hsjday.zip \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  -H "Referer: https://www.tdx.com.cn/" \
  "http://data.tdx.com.cn/vipdoc/hsjday.zip"
```

将 `hsjday.zip` 放到脚本目录下，重启服务即自动解压。

### 健康监控

```bash
# 心跳检测
curl -s http://localhost:8080/api/health | python -m json.tool

# 数据新鲜度检查
curl -s http://localhost:8080/api/GetDataStatus | python -c "
import sys,json; d=json.load(sys.stdin)
print(f'文件: {d[\"total_files\"]}  日期: {d[\"date_range\"][\"start\"]}~{d[\"date_range\"][\"end\"]}')"
```

### 日志

所有操作输出到 stdout，包含时间戳和级别：

```
2026-06-08 16:10:00 [INFO] === 定时下载任务开始 ===
2026-06-08 16:10:02 [INFO] ✓ 下载成功: 536872733 字节
2026-06-08 16:10:15 [INFO] 解压完成
2026-06-08 16:10:15 [INFO]   市场 sh: 5867 个 .day 文件
2026-06-08 16:10:15 [INFO]   市场 sz: 5756 个 .day 文件
2026-06-08 16:10:15 [INFO]   市场 bj: 581 个 .day 文件
```

---

## 附录：接口速查表

| 接口 | 方法 | 关键参数 | 说明 |
|------|------|----------|------|
| `/api/GetSecurityBars` | GET/POST | code, market, count, period, fqtype, start, end | 个股K线 |
| `/api/GetIndexBars` | GET/POST | code, market, count, period | 指数K线 |
| `/api/GetSecurityList` | GET/POST | market | 证券列表 |
| `/api/GetSecurityQuotes` | GET/POST | codes | 最新行情 |
| `/api/GetKLine` | GET/POST | codes, period, count, fqtype | 批量K线 |
| `/api/GetDataStatus` | GET/POST | — | 数据状态 |
| `/api/GetFinanceInfo` | GET/POST | — | 财务信息(预留) |
| `/api/DownloadNow` | **POST** | — | 手动下载 |
| `/api/health` | GET | — | 健康检查 |
