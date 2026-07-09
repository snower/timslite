# 把时序数据直接嵌入应用: timslite 快速开始

很多应用都会在本机不断产生带时间戳的数据。

AI 应用会产生用户消息、模型回复、流式 token、工具调用和推理耗时; 量化系统会持续接收 K 线、逐笔成交和盘口快照; 工业网关会从传感器采集温度、压力、振动和电流; 医疗监护设备会记录心率、血氧、ECG 波形和报警事件; 服务端程序会不断写入结构化日志、trace 和指标。

这些数据有一个共同点: 它们不是传统意义上的业务表, 而是一条条沿时间增长的记录。你通常关心的是:

- 某个时间点有没有数据?
- 最近一条数据是什么?
- 某个时间窗口内发生了什么?
- 本地先写下来, 后面再压缩、消费、同步或回放。

如果为了这些数据单独部署一套时序数据库, 很多桌面工具、边缘设备、本地 AI 应用和单机服务会显得太重; 如果只写普通文件, 又很快会遇到索引、范围查询、压缩、消费进度和历史清理的问题。

timslite 想解决的就是这个中间地带: **一个可以直接嵌入应用的单机时序数据存储库**。

## timslite 是什么

timslite 是一个用 Rust 编写的本地时序数据存储引擎。它不需要启动数据库服务, 也不要求你写 SQL。应用打开一个本地目录, 创建一个 dataset, 然后按 timestamp 写入 bytes。

最小的数据模型可以理解成这样:

```text
Store
└── Dataset: (dataset_name, dataset_type)
    ├── timestamp 1700000001 -> bytes
    ├── timestamp 1700000002 -> bytes
    └── timestamp 1700000003 -> bytes
```

例如:

- `("sensor_001", "temperature")` 保存 1 号传感器的温度采样。
- `("btcusdt", "kline_1m")` 保存 BTC/USDT 的 1 分钟 K 线。
- `("chat_1001", "messages")` 保存某个 AI 会话的消息时间线。
- `("service_api", "error")` 保存 API 服务的错误日志。

每条记录由两部分组成:

- `timestamp: i64`: 业务时间戳或业务序列号。
- `data: bytes`: 由你的应用决定编码方式, 可以是 JSON、UTF-8 文本、Protobuf、MessagePack、压缩后的二进制, 或任何你需要保存的 payload。

timslite 在底层负责本地文件组织、mmap 分段存储、时间索引、Block 聚合、延迟压缩、持久化队列和轻量 journal。对使用者来说, 常用操作就是写入、读取最新、按精确时间读取、按范围查询。

## 它不是 SQLite 的替代品

很多人第一次看到“嵌入式存储库”, 会自然想到 SQLite。这个类比有帮助, 但也容易误导。

SQLite 是一个通用嵌入式关系数据库。它的核心是表、列、SQL、事务、多列索引和关系查询。你可以用它保存用户表、订单表、配置表、任务状态, 也可以做复杂筛选、排序、join 和聚合。

timslite 更专注。它不是为了替代 SQL, 而是为了保存随时间持续增长的数据流。它不关心“按用户名和状态筛选订单”, 它关心“把这个 timestamp 的 payload 写进去”, 以及“把这个时间窗口里的记录读出来”。

可以这样选:

| 需求 | 更适合 |
| --- | --- |
| 保存用户、订单、配置、任务状态等关系型数据 | SQLite |
| 需要 SQL、事务、join、多列条件查询 | SQLite |
| 保存持续产生的 timestamp 数据 | timslite |
| 按时间点读取、按时间窗口扫描 | timslite |
| 本地采集缓存、边缘设备离线写入、后续批量同步 | timslite |
| 大量 payload 主要以 bytes 保存, 由应用自己解析 | timslite |

两者也可以一起用。一个常见搭配是: SQLite 保存元数据、配置和业务状态; timslite 保存大批量时序 payload。比如一个 AI 桌面应用可以用 SQLite 保存会话列表和用户设置, 用 timslite 保存每个会话的消息流、token 流和推理事件。

## 为什么不用普通日志文件

如果只是简单记录文本, 直接 append 到文件当然最轻。但一旦数据需要长期保留和查询, 普通文件会把很多问题留给你自己:

- 时间窗口查询要不要全文件扫描?
- 单个文件越来越大以后如何切分?
- 历史数据如何压缩?
- 读取最近数据和读取历史数据如何兼顾?
- 消费者处理到哪里了, 进度如何持久化?
- 需要同步、审计或回放时, 变更记录从哪里来?

timslite 的价值不在于“能写文件”, 而在于把这些围绕时序数据的基础能力打包成一个可嵌入的库。你的应用仍然掌握数据编码和业务语义, timslite 负责本地时间线存储。

## 适合的使用场景

### AI 消息、token 与推理事件

AI 应用里的数据天然是时间线。

一次会话可能包含用户消息、模型回复、流式 token、工具调用、检索结果、延迟指标和错误事件。很多时候你并不需要对消息内容做复杂 SQL 查询, 而是需要完整回放某个会话、查看某段时间内模型输出了什么、或者把消息流异步同步到远端。

一种建模方式:

| dataset name | dataset type | 保存内容 |
| --- | --- | --- |
| `chat_1001` | `messages` | 用户消息和模型回复 |
| `chat_1001` | `tokens` | 流式 token 或分片输出 |
| `chat_1001` | `events` | tool call、检索、错误、延迟指标 |

payload 可以直接保存 JSON:

```json
{"role":"assistant","content":"你好, 我可以帮你分析这段日志。"}
```

这样做的好处是, 你的 AI 应用可以先把所有过程可靠落到本地, 后续再做摘要、同步、审计或回放。

### K 线、行情与交易事件

行情数据也是典型的时序数据。K 线的 timestamp 通常是周期开始时间, 逐笔成交的 timestamp 是成交时间, 盘口快照的 timestamp 是采样时间。

一种建模方式:

| dataset name | dataset type | 保存内容 |
| --- | --- | --- |
| `btcusdt` | `kline_1m` | 1 分钟 K 线 |
| `btcusdt` | `kline_5m` | 5 分钟 K 线 |
| `btcusdt` | `trade` | 逐笔成交 |
| `btcusdt` | `depth` | 盘口快照 |

应用可以把每条 K 线编码成 JSON、bincode、MessagePack 或 Protobuf。读取时通常就是“给我 09:30 到 10:00 的数据”, 这正是 timslite 的查询模型。

### 工业传感器数据采集

边缘网关和工控设备经常会遇到网络不稳定、上位系统不可用、现场数据不能丢的问题。timslite 可以作为本地采集缓存: 设备先把传感器数据写到本地目录, 后台任务再按时间窗口上传。

一种建模方式:

| dataset name | dataset type | 保存内容 |
| --- | --- | --- |
| `device_17` | `temperature` | 温度采样 |
| `device_17` | `pressure` | 压力采样 |
| `device_17` | `vibration` | 振动数据 |

当网络恢复时, 应用可以查询某个时间范围的数据并发送到中心系统。配合持久化 queue, 还可以维护消费者进度, 避免重复处理或漏处理。

### 医疗监护数据采集

医疗监护设备同样会持续产生时序数据, 例如心率、血氧、血压、ECG 波形和报警事件。timslite 适合做设备侧或应用侧的本地采集缓存和短期回看窗口。

一种建模方式:

| dataset name | dataset type | 保存内容 |
| --- | --- | --- |
| `monitor_03` | `heart_rate` | 心率 |
| `monitor_03` | `spo2` | 血氧 |
| `monitor_03` | `ecg` | ECG 波形片段 |
| `monitor_03` | `alarm` | 报警事件 |

需要注意的是, timslite 是本地时序存储库, 不是电子病历系统, 也不替代合规审计、权限管理和医疗数据治理平台。它更适合作为采集链路中的本地存储组件。

### 结构化日志和本地 trace

日志看起来像文本, 但真正处理时通常也是时间线: 最近发生了什么、某段时间有哪些错误、一个 trace 的事件顺序是什么。

一种建模方式:

| dataset name | dataset type | 保存内容 |
| --- | --- | --- |
| `service_api` | `info` | 普通日志 |
| `service_api` | `error` | 错误日志 |
| `service_api` | `trace` | trace event |

对于单机应用、桌面软件、本地 agent 和边缘服务来说, timslite 可以作为轻量日志存储层。你可以把结构化日志保存成 JSON bytes, 再按时间窗口读取、展示或上传。

## 快速开始: Rust

timslite 的主项目是标准 Rust library。添加依赖:

```toml
[dependencies]
timslite = "0.1.2"
```

下面的例子创建一个本地 store, 再创建一个温度 dataset, 写入两条采样数据, 然后按时间点和时间范围读取:

```rust
use timslite::{DataSetConfigBuilder, Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let store_config = StoreConfig::builder()
        .enable_background_thread(true)
        .enable_journal(true)
        .build();

    let mut store = Store::open("./data/timslite", store_config.clone())?;

    let dataset_config = DataSetConfigBuilder::from_store(&store_config)
        .index_continuous(0)
        .retention_window(0);

    let mut temperature = store.create_dataset_with_config(
        "sensor_001",
        "temperature",
        Some(dataset_config),
    )?;

    temperature.write(1_700_000_001, b"21.5")?;
    temperature.write(1_700_000_002, b"21.7")?;

    if let Some((ts, data)) = temperature.read(1_700_000_001)? {
        println!("point: ts={ts}, value={}", String::from_utf8_lossy(&data));
    }

    for (ts, data) in temperature.query(1_700_000_000, 1_700_000_010)? {
        println!("range: ts={ts}, value={}", String::from_utf8_lossy(&data));
    }

    store.close()?;
    Ok(())
}
```

这段代码里有三个核心概念:

- `Store`: 一个本地数据目录, 负责 dataset 生命周期、缓存、后台任务和 journal。
- `DataSet`: 一个具体时间线, 由 `(name, type)` 唯一标识。
- `timestamp + bytes`: 你写入和读取的基本记录。

打开已有 dataset 时, 不需要重新创建:

```rust
use timslite::{Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let mut store = Store::open("./data/timslite", StoreConfig::default())?;
    let temperature = store.open_dataset("sensor_001", "temperature")?;

    if let Some((ts, data)) = temperature.read_latest()? {
        println!("latest: ts={ts}, value={}", String::from_utf8_lossy(&data));
    }

    Ok(())
}
```

## 快速开始: Python

如果你在写采集脚本、AI 原型或本地工具, 可以直接用 Python wrapper:

```bash
python -m pip install timslite
```

下面用一个 AI 会话作为例子:

```python
import timslite

with timslite.Store.open("./data/timslite") as store:
    store.create_dataset("chat_1001", "messages")
    messages = store.open_dataset("chat_1001", "messages")

    messages.write(1700000001, b'{"role":"user","content":"hello"}')
    messages.write(1700000002, b'{"role":"assistant","content":"hi"}')

    latest = messages.read_latest()
    print("latest:", latest)

    for ts, data in messages.query(1700000000, 1700000010):
        print(ts, data.decode("utf-8"))
```

Python 版本的心智模型和 Rust 一样: 打开 store, 创建或打开 dataset, 按 timestamp 写入 bytes, 再按时间读取。

## 其他语言集成

除了 Rust 和 Python, timslite 也提供 Node.js、Java 和 .NET wrapper。不同语言面对的是同一套核心存储模型:

- Rust: 适合对性能、生命周期和部署体积要求高的服务或系统组件。
- Python: 适合采集脚本、AI 原型、本地数据处理工具。
- Node.js: 适合 Electron、桌面工具、本地 agent 和 JavaScript 服务。
- Java/.NET: 适合已有企业应用、网关程序或桌面客户端集成。

如果你的系统里有多种语言, 可以让核心采集层用 Rust 写入, 上层工具用 Python 或 Node.js 做回放、导出和分析。

## 建模时怎么拆 dataset

timslite 的 dataset 由 `dataset_name` 和 `dataset_type` 两段组成。一个实用原则是:

- `dataset_name` 表示主体: 设备、交易对、会话、服务、用户、通道。
- `dataset_type` 表示数据流类型: 温度、K 线、消息、日志级别、事件类别。

例如:

| 业务 | dataset name | dataset type |
| --- | --- | --- |
| 温度传感器 | `sensor_001` | `temperature` |
| BTC 1 分钟 K 线 | `btcusdt` | `kline_1m` |
| AI 会话消息 | `chat_1001` | `messages` |
| API 错误日志 | `service_api` | `error` |
| ECG 波形片段 | `monitor_03` | `ecg` |

timestamp 的单位由你自己决定, 可以是秒、毫秒、微秒, 也可以是业务序列号。关键是同一个 dataset 内保持一致。payload 的编码也由你决定; timslite 不解析 payload, 只把它作为 bytes 存储和返回。

公开的 dataset name、dataset type 和 queue group name 需要保持简单, 只使用数字、大小写英文字母、`-` 和 `_`。例如 `sensor_001`、`kline_1m`、`chat-1001` 都是合理命名。

## 进一步可以用到的能力

快速开始只展示了最基础的写入和查询。实际项目里, 你还可能用到:

- **范围查询迭代器**: 大范围读取时逐条消费, 避免一次性加载太多数据。
- **持久化 queue**: 让后台消费者按进度处理 dataset 中的新数据。
- **journal**: 记录 dataset create/drop/write/delete/append 等变更, 用于同步、审计或回放。
- **retention window**: 为只需要短期数据的场景设置保留窗口。
- **后台任务**: 处理 flush、idle close、缓存回收和保留策略。

这些能力的目标都是同一个: 让应用可以把本地时序数据可靠地写下来, 再按自己的节奏查询、消费和同步。

## 什么时候不该用 timslite

timslite 的定位很窄, 这也是它清晰的地方。下面这些场景不应该优先选它:

- 你需要复杂 SQL、join、多列条件查询和事务。
- 你需要多节点分布式时序数据库。
- 你需要完整的权限体系、审计平台或医疗数据治理系统。
- 你的数据主要是低频配置、业务对象和关系表。
- 你的主要需求是全文检索或模糊搜索。

这些场景里, SQLite、PostgreSQL、ClickHouse、InfluxDB、OpenSearch 等系统会更合适。

timslite 更适合这样的任务: **在一个应用或设备内部, 高效保存不断增长的本地时间线数据**。

## 总结

timslite 可以被理解成“给应用内置的一条条时间线”。它不像 SQLite 那样提供通用关系模型, 也不像大型时序数据库那样要求独立部署。它做的是更小、更贴近本地应用的一件事: 把 timestamp + bytes 可靠地写入本地, 并让你按时间点和时间窗口快速读回来。

如果你正在做 AI 消息记录、行情缓存、工业采集、医疗监护采样、日志存储、本地 agent 或边缘设备, 并且希望数据先稳稳落在本机, timslite 值得试一下。
