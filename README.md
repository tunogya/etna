0. 总体架构（你最终会组装成两条管线）
	1.	离线/回填管线（Backfill / Batch）
从历史 K 线构建窗口 → 特征/归一化 → embedding → 写 DuckDB + 写 Milvus
	2.	实时管线（Streaming / Online）
订阅最新 K 线 → 增量维护窗口 → embedding → 写入 DuckDB + Milvus →（可选）触发查询与统计

两条管线共用同一套“窗口构建、特征、embedding、写入”组件，只是输入不同（历史 vs 实时）。

⸻

1) 核心模块清单（按职责边界拆）

M1. Market Data 输入层

职责：统一输出标准 K 线结构 Candle，供后续模块消费
接口建议：
	•	BackfillProvider: 读取历史（文件/接口/导入）
	•	StreamProvider: 实时订阅（WebSocket/轮询）

输出结构（示意）：
	•	symbol, timeframe, open_time, close_time, o,h,l,c,v, trades(optional), vwap(optional)

先不纠结数据源，模块接口先定好：BTCUSDT 的数据你可以先用本地 CSV/Parquet 导入，实时再接交易所 WS。

⸻

M2. Window Builder（滑动窗口管理器）

职责：把连续 K 线变成固定长度窗口（rolling window），并支持实时增量更新
关键点：
	•	参数：W（窗口长度）、S（步长）、Warmup（需要多少根才开始产出窗口）
	•	实时：每来一根新 K 线，就更新 ring buffer；若满足 W 就产出 1 个新窗口（或按 S 产出）

输出结构：
	•	Window{window_id, symbol, tf, t_end, candles[W]}

window_id 规范（强烈建议一开始定死）：
	•	window_id = hash(symbol|tf|t_end|W|feature_version)
	•	保证幂等写入（重复处理不怕）

⸻

M3. Feature/Normalization（结构化归一化）

职责：把 candles[W] 转换为：
	1.	结构化特征（可过滤/统计）
	2.	可 embedding 的定长数值向量（shape vector）

输出建议分两类：
	•	FeatureRow（列式字段）：trend_slope, rv, mdd, atr, vol_z, ...
	•	ShapeVector（float32[]）：例如拼接 returns + wick + range (+vol_z) 得到 96/128 维

你前期可以直接用“拼接向量”做 embedding 输入（或直接作为 Milvus 向量），后续再迭代更强表示。

⸻

M4. Embedding Client（Ollama）

职责：把 ShapeVector 或序列编码为 embedding，输出 []float32
注意：Ollama 的 embedding 模型一般接受文本输入。你有两条路径：
	•	路径 A（推荐先跑通）：不依赖 LLM embedding
直接把 ShapeVector（已归一化）作为 Milvus 向量存储（即“手工 embedding”），距离用 cosine/L2。
这条路简单、速度快、可控，先把系统闭环跑起来。
	•	路径 B（你现在设想的）：Ollama embedding
把窗口序列编码成文本/JSON（会增加 token 成本与噪声），再调用 embedding。
	•	优点：可能学到更抽象相似性
	•	缺点：速度/成本/一致性风险更大，且对数值序列表达不天然

务实建议：先用路径 A 把 Milvus 检索与后验统计做出来；Ollama embedding 作为可插拔实现，后续再对比效果。

⸻

M5. DuckDB Storage（事实表 + 特征表）

职责：持久化原始 K 线、窗口、特征、结果统计；支持 SQL 过滤与回放
最小表设计（BTCUSDT 先做最小可用）：
	1.	candles（事实表）

	•	symbol, tf, open_time, open, high, low, close, volume, ...
	•	主键：(symbol, tf, open_time)

	2.	windows（窗口索引表，可选但推荐）

	•	window_id, symbol, tf, t_end, W, feature_version, created_at

	3.	window_features

	•	window_id, ... feature columns ..., vol_bucket, trend_bucket, data_version
	•	这里放你要做过滤的字段（尤其是后面“时间权重”的分桶字段）

	4.	（可选）window_outcomes

	•	window_id, horizon, fwd_ret_mean, fwd_ret_p50, mdd_p95 ...
	•	用于缓存后验统计结果，避免每次查询都算

⸻

M6. Milvus Vector Store（相似检索）

职责：存储 embedding + payload（用于过滤）并执行 TopK
collection 设计建议：kline_windows

字段：
	•	window_id（主键，string/varchar 或 int64 hash）
	•	embedding（float_vector[dim]）
	•	symbol、tf
	•	t_end（int64 timestamp）
	•	vol_bucket、trend_bucket（int）
	•	data_version（int）

检索模式：
	•	先 filter：symbol == "BTCUSDT" AND tf == "1m" AND data_version == X AND t_end >= now-30d
	•	再 TopK：embedding 近邻

“时间权重”不要一开始强行写进向量距离；先用 filter + rerank 更可控。

⸻

M7. Time Weighting & Reranker（时间优先策略）

你提出的关键需求：优先处理最近几天/一月数据，并且实时更新。

拆成两件事：

7.1 数据处理优先级（ingest 优先级）
	•	Backfill 时：按时间倒序回填（最近 → 更早）
	•	这样你很快就能用“最近数据”提供服务，不必等全量历史跑完

7.2 检索结果时间加权（query rerank）
Milvus 返回 TopK 相似窗口后，你做二次排序：
	•	final_score = sim_score * time_decay(t_end)
	•	time_decay = exp(-lambda * age_days) 或 1 / (1 + age_days)
	•	你还可以做分段权重：
	•	最近 3 天：×1.0
	•	最近 30 天：×0.7
	•	更早：×0.4

这比改向量距离更稳、更容易调参。

⸻

M8. Outcome Engine（后验分布统计）

职责：对 TopK 近邻窗口，统计未来 N 根的收益/回撤分布
输入：
	•	neighbors window_id[]
	•	horizons = [5, 20, 60]（按你的策略）

实现：
	•	从 DuckDB 拉对应 t_end 之后的价格序列计算 forward returns / MDD
	•	输出分位数（p10/p50/p90）、均值、最差情况等

这个模块会是你最终“可交易决策辅助”的核心。

⸻

2) 先做 BTCUSDT 的“最小可用核心功能”里程碑（建议你按这个顺序实现）

Milestone A：离线闭环（不含实时）
	1.	导入 BTCUSDT 历史 candles → DuckDB candles
	2.	Window Builder 产出窗口 → windows
	3.	Feature/Normalization → window_features
	4.	写 Milvus（embedding 用 ShapeVector 直接入库）
	5.	给一个“当前窗口”做 TopK 检索，拿到 window_id 列表
	6.	Outcome Engine 计算 +20 根的收益分布，打印结果

你完成 A，就已经证明：数据管线 + 向量检索 + 后验统计是通的。

Milestone B：时间优先与缓存
	1.	Backfill 改为最近 30 天优先（倒序）
	2.	检索加 rerank（时间衰减）
	3.	把 outcome 结果写入 DuckDB 缓存（window_outcomes）

Milestone C：实时增量
	1.	StreamProvider 接入实时 candle
	2.	Window Builder 增量产窗
	3.	新窗口实时写 DuckDB + Milvus
	4.	（可选）每来一根新 K 线自动跑一次“当前窗口→检索→统计”

⸻

3) 你现在就能确定的关键参数（先定死，后续版本化迭代）
	•	tf: 建议先 1m 或 5m 选一个（不建议同时开）
	•	W: 1m 先用 60；5m 先用 24
	•	S: 离线回填先用 5（降数据量），实时用 1
	•	dim: ShapeVector 拼接维度先做成 96 或 128（便于 Milvus）
	•	data_version: 从 1 开始，每次改特征/归一化/维度就 +1（避免库里混杂）

⸻

4) 关于 “最近数据优先处理” 的实现方式（你会用得上）

方式 1：两段队列（推荐）
	•	hot_queue: 最近 3 天/7 天，优先处理
	•	warm_queue: 最近 30 天
	•	cold_queue: 更早（可选）

Backfill 时从 hot→warm→cold，实时进入 hot。

方式 2：单队列优先级（Priority Queue）

任务粒度为“时间分片”（比如每 1 天或每 1 小时一个 batch）：
	•	priority = -timestamp（越新越小，越优先）
	•	worker pool 拉取执行

⸻

5) 下一步你最应该做的“模块边界文件夹结构”（Go 项目骨架）

你可以按如下方式拆包（便于独立测试）：
	•	pkg/model：Candle/Window/FeatureRow
	•	pkg/data：BackfillProvider/StreamProvider
	•	pkg/window：WindowBuilder（ring buffer）
	•	pkg/feature：normalize + feature calc
	•	pkg/embed：Ollama client（可插拔），以及 IdentityEmbedder（直接用 ShapeVector）
	•	pkg/store/duckdb：schema + upsert + query
	•	pkg/store/milvus：collection + insert + search
	•	pkg/rerank：time decay
	•	pkg/outcome：forward returns / MDD
	•	cmd/backfill：回填入口
	•	cmd/stream：实时入口
	•	cmd/api：（可选）对外查询接口
