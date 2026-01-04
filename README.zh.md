# Etna

基于 Go 语言的加密货币市场分析 K 线数据处理管道，支持滑动窗口处理、向量相似度搜索和前向收益统计。

## 概述

Etna 提供两种主要的数据处理管道：

1. **批处理管道 (Backfill)**：处理历史 K 线数据 → 构建窗口 → 生成特征 → 存储到 DuckDB + Milvus
2. **流式管道 (实时)**：订阅实时 K 线数据 → 增量窗口更新 → 实时存储和分析

两种管道共享相同的核心组件：窗口构建、特征提取、向量嵌入和存储。

## 架构

```
市场数据 → 窗口构建器 → 特征/归一化 → 向量存储 → 收益分析
```

### 核心模块

| 模块 | 描述 |
|------|------|
| **市场数据** | 统一的 K 线输入层（批量/流式数据提供者） |
| **窗口构建器** | 可配置长度和步长的滑动窗口管理器 |
| **特征引擎** | 归一化蜡烛图数据并提取结构化特征 |
| **DuckDB 存储** | 持久化原始蜡烛图、窗口和特征，支持 SQL 查询 |
| **Milvus 向量存储** | 存储嵌入向量，支持 TopK 相似度搜索 |
| **时间加权 & 重排序器** | 基于衰减的排序优先处理最近数据 |
| **收益引擎** | 计算前向收益和风险指标 |

## 项目结构

```
pkg/
├── model/       # 核心数据结构（Candle, Window, FeatureRow）
├── data/        # 数据提供者（BackfillProvider, StreamProvider）
├── window/      # 基于环形缓冲区的窗口构建器
├── feature/     # 特征计算和归一化
├── embed/       # 嵌入实现（IdentityEmbedder）
├── store/
│   ├── duckdb/  # DuckDB 模式定义、更新插入和查询操作
│   └── milvus/  # Milvus 集合管理和搜索
├── rerank/      # 时间衰减重排序
└── outcome/     # 前向收益和最大回撤计算

cmd/
├── backfill/    # 批处理入口
├── stream/      # 实时处理入口
└── api/         # 查询接口（可选）
```

## 核心概念

### 窗口配置

| 参数 | 描述 | 推荐值 |
|------|------|--------|
| `tf` | 时间框架 | `1m` 或 `5m`（二选一） |
| `W` | 窗口长度 | 1m 为 60，5m 为 24 |
| `S` | 步长 | 批处理为 5，流式为 1 |
| `dim` | 向量维度 | 96 或 128 |
| `data_version` | 模式版本 | 从 1 开始，变更时递增 |

### 窗口 ID 生成

```
window_id = hash(symbol | tf | t_end | W | feature_version)
```

这确保了幂等写入并防止重复处理。

### 时间加权处理

**数据摄入优先级：**
- 按时间倒序回填（最近 → 历史）
- 新数据更快可供查询

**查询重排序：**
```
final_score = similarity_score × time_decay(t_end)
time_decay = exp(-λ × age_days)
```

或使用分段权重：
- 最近 3 天：×1.0
- 最近 30 天：×0.7
- 更早：×0.4

## 数据库模式

### DuckDB 表

**candles**（事实表）
```sql
CREATE TABLE candles (
    symbol VARCHAR,
    tf VARCHAR,
    open_time TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    PRIMARY KEY (symbol, tf, open_time)
);
```

**windows**（索引表）
```sql
CREATE TABLE windows (
    window_id VARCHAR PRIMARY KEY,
    symbol VARCHAR,
    tf VARCHAR,
    t_end TIMESTAMP,
    W INTEGER,
    feature_version INTEGER,
    created_at TIMESTAMP
);
```

**window_features**（特征表）
```sql
CREATE TABLE window_features (
    window_id VARCHAR PRIMARY KEY,
    trend_slope DOUBLE,
    realized_volatility DOUBLE,
    max_drawdown DOUBLE,
    atr DOUBLE,
    vol_bucket INTEGER,
    trend_bucket INTEGER,
    data_version INTEGER
);
```

**window_outcomes**（缓存表，可选）
```sql
CREATE TABLE window_outcomes (
    window_id VARCHAR,
    horizon INTEGER,
    fwd_ret_mean DOUBLE,
    fwd_ret_p50 DOUBLE,
    mdd_p95 DOUBLE,
    PRIMARY KEY (window_id, horizon)
);
```

### Milvus 集合

**集合：** `kline_windows`

| 字段 | 类型 | 描述 |
|------|------|------|
| `window_id` | VARCHAR (主键) | 唯一窗口标识符 |
| `embedding` | FLOAT_VECTOR[dim] | 特征向量 |
| `symbol` | VARCHAR | 交易对 |
| `tf` | VARCHAR | 时间框架 |
| `t_end` | INT64 | 结束时间戳 |
| `vol_bucket` | INT | 波动率分桶 |
| `trend_bucket` | INT | 趋势分桶 |
| `data_version` | INT | 模式版本 |

**搜索模式：**
```
filter: symbol == "BTCUSDT" AND tf == "1m" AND data_version == X AND t_end >= now - 30d
then: TopK nearest neighbors on embedding
```

## 里程碑

### 里程碑 A：批处理管道（无实时）

1. ✅ 导入历史 BTCUSDT 蜡烛图 → DuckDB
2. ✅ 窗口构建器生成窗口
3. ✅ 特征/归一化 → window_features
4. ✅ 写入 Milvus（直接使用 ShapeVector）
5. ✅ 对给定窗口进行 TopK 搜索
6. ✅ 收益引擎计算 +20 根 K 线的前向收益

### 里程碑 B：时间优先级 & 缓存

1. ⬜ 按时间倒序回填
2. ⬜ 在搜索中添加时间衰减重排序
3. ⬜ 将收益结果缓存到 window_outcomes

### 里程碑 C：实时流处理

1. ⬜ 实现实时蜡烛图的 StreamProvider
2. ⬜ 增量窗口生成
3. ⬜ 实时写入 DuckDB + Milvus
4. ⬜ 新蜡烛图自动触发搜索和分析

## 快速开始

### 前置要求

- Go 1.25+
- DuckDB
- Milvus 2.x

### 安装

```bash
git clone https://github.com/tunogya/etna.git
cd etna
go mod tidy
```

### 使用方法

```bash
# 运行批处理管道
go run cmd/backfill/main.go

# 运行流式管道
go run cmd/stream/main.go

# 启动 API 服务器（可选）
go run cmd/api/main.go
```

## 许可证

MIT 许可证
