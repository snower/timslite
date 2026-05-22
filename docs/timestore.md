# TimeStore 时序数据存储系统详解

> 项目: monitorcare-orbit (Spring Boot)  
> 模块: `com.nalong.monitorcare.orbit.handler.filesystem`  
> 特性: 仅支持追加写入和按时间范围读取,不支持更新/删除

---

## 一、整体架构概览

```
FileManager (门面/管理器)
    │
    ├───RocksPersistence (RocksDB持久化 - 设备索引与元数据)
    │
    └───filesystem.core
            ├───RecordMonitorFile (单次记录容器 - 管理所有文件类型队列)
            │       ├───QueueMonitorFile × 6 (按数据类型组织的Mapped文件队列)
            │       │       └───MonitorFile × N (单个内存映射文件)
            │       │               └───MonitorFileReader (文件的只读视图)
            │       └───RecordMonitorWarFile (可选的预写日志/WAR模块)
            │               ├───WarStateFile (推送状态跟踪文件)
            │               └───RecordMonitorWarReader (WAR数据读取器)
            └───filesystem.index (索引迭代器)
                    ├───IndexInfo (索引记录: waveOffset + measureOffset + timestamp)
                    ├───IndexInfoFileIterator (单文件索引遍历)
                    └───IndexInfoIterator (多文件索引遍历)
```

**核心设计思想**: 按数据类型分段 + 内存映射文件(MappedByteBuffer) + 预写日志(WAL)

---

## 二、文件目录结构

```
{storePath}/
├── device.pbb                              # 设备描述(protobuf序列化)
├── patient.pbb                             # 患者描述(protobuf序列化)
│
├── record/                                 # RecordMonitorFile管理
│   ├── {recordId}/
│   │   ├── meta.pbb                        # RecordMetaData(protobuf)
│   │   ├── war.pbb                         # WarStateFile(推送状态跟踪)
│   │   ├── 0/                              # Type 0: 索引文件(按timestamp命名)
│   │   │   ├── 00000017000000000000        # 文件名 = 起始秒级时间戳(20位补零)
│   │   │   └── 00000017000000600000
│   │   ├── 1/                              # Type 1: 测值波形(按offset命名)
│   │   │   ├── 00000000000000000000        # 文件名 = 文件起始偏移量(20位补零)
│   │   │   ├── 00000000000000067108864     # 偏移量 = 64MB
│   │   │   └── 00000000000000134217728
│   │   ├── 2/                              # Type 2: 测值数据(按offset命名,32MB/文件)
│   │   ├── 3/                              # Type 3: 事件数据(按timestamp命名,8MB/文件)
│   │   ├── 4/                              # Type 4: WAR预写日志(按offset命名,16MB/文件)
│   │   └── 5/                              # Type 5: 手动测值(按timestamp命名,8MB/文件)
│
└── push/                                   # 推送状态
    └── {deviceCode}/{recordId}/
        └── node.pbb                        # 每个推送节点的元数据
```

### 2.1 文件命名规则

| 数据类型 | 文件类型值 | 单文件大小 | 命名依据 | 示例 |
|---------|-----------|-----------|---------|------|
| 索引 | 0 | 16 MB | 秒级时间戳 | `00000017000000000000` |
| 波形 | 1 | 64 MB | 字节偏移量 | `00000000000000000000` |
| 测值 | 2 | 32 MB | 字节偏移量 | `00000000000000000000` |
| 事件 | 3 | 8 MB | 秒级时间戳 | `00000017000000000000` |
| WAR日志 | 4 | 16 MB | 字节偏移量 | `00000000000000000000` |
| 手动测值 | 5 | 8 MB | 秒级时间戳 | `00000017000000000000` |

> 文件名统一使用 `NumberFormat` 格式化20位数字,前补零。

---

## 三、MonitorFile: 单个内存映射文件

### 3.1 文件布局

```
┌─────────────────────────────────────────────────┐
│ FileHeader (Protobuf序列化)                      │
│ - magic: "NMPB" (4 bytes)                        │
│ - metaSize: int (4 bytes)                        │
│ - metadata bytes (可变长度)                       │
├─────────────────────────────────────────────────┤
│ State Area (仅WAR文件, 8 bytes)                   │
│ - currentWriteOffset: long (4+4+metaSize之后8字节)│
├─────────────────────────────────────────────────┤
│ Index Area (仅事件/手动测值, 262KB)               │
│ - 二级索引: timestamp → dataOffset (16 bytes/条) │
│ - 最多16384条索引                                 │
├─────────────────────────────────────────────────┤
│ Data Area (主数据区)                              │
│ ├─ [size:4][protobuf data][size:4][protobuf data]│ (未压缩,波形/测值)
│ └─ [size:4][compressed data][size:4][compressed] │ (压缩,事件/手动测值)
└─────────────────────────────────────────────────┘
```

### 3.2 文件头元数据 (FileMetadata protobuf)

```protobuf
FileMetadata {
  int32 version = 1;          // 版本号,当前为1
  int64 timestamp = 2;        // 文件创建时间(ms)
  int64 recordId = 3;         // 记录ID
  string deviceCompanyCode = 4;
  string deviceModelCode = 5;
  string deviceCode = 6;
  int32 recordType = 7;       // 0=索引,1=波形,2=测值,3=事件,4=WAR,5=手动测值
  int64 fileOffset = 8;       // 文件起始偏移量/时间戳
  int64 fileSize = 9;         // 文件总大小
  int32 stateSize = 10;       // 状态区大小 (WAR文件=8)
  int32 compressType = 11;    // 0=不压缩,1=deflate
  int32 compressLevel = 12;   // 压缩级别 (默认7)
  int64 indexSize = 13;       // 索引区大小 (262144)
  int64 indexCount = 14;      // 索引条数上限 (16384)
}
```

### 3.3 核心写入流程 (appendData)

```java
// 1. 数据写入: 先写4字节大小头 + 数据体
mappedByteBuffer.putInt(size);     // 前置大小标记
mappedByteBuffer.put(buffer);      // 实际数据
writeSize += size + 4;

// 2. 更新文件状态头中的已写入位置
mappedByteBuffer.putInt(headerSize + 4, wrotePosition);

// 3. 更新二级索引(仅事件/手动测值)
// 从protobuf中提取timestamp field,写入[IndexArea](timestamp + dataPosition)

// 4. 同步WAL日志(如果启用)
// warFile.appendXxxDataWar(device, monitorFile, dataOffset)
```

### 3.4 数据读取 (MonitorFileReader)

MonitorFileReader是MonitorFile的内部类,提供只读视图:

```java
public static class MonitorFileReader implements AutoCloseable {
    // 使用 asReadOnlyBuffer() 防止并发写干扰
    private MappedByteBuffer readOnlyBuffer;

    // 关键API:
    int readInt(int position);          // 读4字节
    long readLong(int position);        // 读8字节
    int readSize(int position);         // 读数据块大小头
    int readBuffer(int position, BufferUtils.Buffer, Inflater);  // 读+解压缩
}
```

> **压缩支持**: fileType 1(波形), 2(测值), 3(事件), 5(手动测值) 使用 deflate 压缩(compressType=1)。读取时通过 `Inflater(true)` (nowrap=true,原始deflate)解压。

---

## 四、QueueMonitorFile: 数据类型文件队列

### 4.1 职责

- 管理同一数据类型的多个MonitorFile,形成逻辑上的无限队列
- 按时间戳/偏移量路由到正确的文件
- 自动创建新文件(当当前文件满时)
- 懒加载/懒关闭(30分钟未使用自动关闭)
- 数据过期清理(cleanUp)

### 4.2 文件查找策略

#### 按时间戳查找索引文件 (`getIndexMonitorFileByTimestamp`)

```
输入: timestamp (秒级)
1. 遍历mappedFiles找到 fileFromOffset <= timestamp 的最近文件
2. 计算 timestampPosition = dataPosition + (timestamp - fileFromOffset) * 24
3. 若timestampPosition >= fileSize → 创建新文件,填充空白索引
4. 若timestampPosition >= wrotePosition → 从最后一个已写位置补齐空白索引
5. 验证索引中timestamp字段匹配,返回文件
```

> **空白索引填充**: 当写入时间戳跳跃时,自动用 `(-1, -1, currentTimestamp)` 填充中间缺失的秒,保证索引连续性。每个索引条目固定24字节。

#### 按时间戳范围查找 (`findMappedFilesByTimespan`)

```
扫描所有文件,判断 [startTimespan, endTimespan] 与 [fileStart, fileEnd] 是否有交集
```

### 4.3 索引文件格式

每条索引记录固定 **24字节**:
```
┌──────────────┬──────────────┬──────────────┐
│ waveOffset   │ measureOffset│ timestamp    │
│ (8 bytes)    │ (8 bytes)    │ (8 bytes)    │
└──────────────┴──────────────┴──────────────┘
```

- `waveOffset`: 对应波形数据的绝对字节偏移
- `measureOffset`: 对应测值数据的绝对字节偏移  
- `timestamp`: 秒级时间戳

### 4.4 事件/手动测值的二级索引

对于type 3(事件)和type 5(手动测值),文件内额外维护262KB的二级索引区:
```
[IndexArea]
┌──────────────┬──────────────┬──────────────┬─────────┐
│ timestamp    │ dataPosition │ timestamp    │ position│ ...
│ (8 bytes)    │ (8 bytes)    │ (8 bytes)    │ (8 bytes)│ ...
└──────────────┴──────────────┴──────────────┴─────────┘
最多16384条
```

写入时自动更新索引,读取时通过索引快速定位数据块位置。

---

## 五、RecordMonitorFile: 记录级容器

### 5.1 职责

- 管理单次监护记录(recordId)的所有数据
- 管理meta.pbb元数据(记录开始/结束时间,上下线状态)
- 管理6种QueueMonitorFile的懒加载
- 管理device.pbb(设备信息)和patient.pbb(患者信息)的读写
- 通过RocksDB管理手动测值和事件的状态持久化
- 空闲超时检查(30分钟)

### 5.2 生命周期

```
创建 → 初始化meta.pbb → 写入各类数据 → 标记recordFinish → 数据过期清理 → 删除
```

### 5.3 Records状态机(meta.pbb)

```protobuf
RecordMetaData {
  int64 recordId, deviceId, patientId;
  string deviceCompanyCode, deviceModelCode, deviceCode;
  int64 recordStartTimestamp;     // 记录开始时间
  int64 recordFinishTimestamp;    // 记录结束时间 (>0表示已完成)
  int64 lastOnlineTimestamp;      // 最后上线时间
  int64 lastOfflineTimestamp;     // 最后离线时间
}
```

### 5.4 RocksDB集成的状态管理

RecordMonitorFile对两类数据使用RocksDB做**内存外状态存储**:

1. **手动测值** (`DeviceMonitorMeasureManualDataPersistence`):  
   按code去重,只保留最新的测值。记录完成后从RocksDB删除。
   
2. **事件** (`DeviceMonitorEventPersistence`):  
   支持actionType: 1=创建/更新, 2=删除。记录完成后从RocksDB删除。

> 这两类数据不直接存为时序文件,而是保持在RocksDB中,直到记录完成后才清除。

---

## 六、WAR(Write Ahead Record)预写日志模块

### 6.1 设计目的

WAR模块是一个**变更日志系统**,记录对record的所有数据变更操作,用于数据推送/同步/复制。不是传统数据库的WAL,更像是一个**操作日志队列**。

### 6.2 WAR条目格式

每个WAR条目固定 **16字节**:
```
┌──────────┬──────────────────┬─────────────────┐
│ warType  │ fileStartOffset  │ fileDataOffset  │
│ (4 bytes)│ (8 bytes)        │ (4 bytes)       │
└──────────┴──────────────────┴─────────────────┘
```

| warType | 值 | 含义 |
|---------|---|------|
| None | 0 | 无操作 |
| RecordMetaData | 1 | 元数据变更 |
| RecordDeviceData | 2 | 设备数据变更 |
| RecordPatientData | 3 | 患者数据变更 |
| MeasureWaveData | 4 | 波形数据写入 |
| EventData | 5 | 事件数据变更 |
| MeasureManualData | 6 | 手动测值变更 |

### 6.3 WarStateFile: 推送状态跟踪

WarStateFile跟踪多个消费节点的读取进度:

```
WarStateFile(4KB)
├── Header: WarFileMetadata (protobuf)
├── State Area(8 bytes): 当前写入位置
└── Data Area: 多个WarState条目
        └── WarStateHeader(protobuf): sourceNodeId + destinationNodeId
            + 8 bytes: 已消费到的warOffset
```

关键API:
- `updateCurrentOffset(warOffset)`: 更新最新写入位置 (sourceNode="", destNode="")
- `updatePushOffset(nodeId, warOffset)`: 记录某节点已推送位置 (sourceNode=nodeId, destNode="")
- `updatePushAckOffset(nodeId, warOffset)`: 记录某节点已确认位置 (sourceNode="", destNode=nodeId)

### 6.4 RecordMonitorWarReader: 按WAR日志回放

```java
WarDataItem item = warReader.next();  // 遍历WAR条目
switch (item.getWarType()) {
    case MeasureWaveData: 
        MonitorRealTimeData data = waveReader.loadMeasureWaveData(item);
        // → 通过item.fileStartOffset找到索引文件
        // → 读取waveOffset → 在波形文件中读取+解压
        // → 读取measureOffset → 在测值文件中读取+解压
        // → 组装MonitorRealTimeData
    case EventData: 
        MonitorEventRealTimeData data = eventReader.loadEventData(item);
    // ...
}
```

---

## 七、索引迭代器(读取核心)

### 7.1 IndexInfoFileIterator (单文件迭代)

遍历单个索引文件中的IndexInfo记录,支持:
- **顺序遍历**: 从头到尾读取所有索引
- **时间范围查找**: 给定[startTimestamp, endTimestamp]直接定位
- **二分查找优化**: 当startTimestamp与文件起始相差>128秒且文件>128条时,使用**二分搜索**快速定位起始位置

```java
// 二分查找逻辑
minIndex = 0, maxIndex = count - 1
index = maxIndex / 2
while (true) {
    currentIndexInfo = read(index);
    if (startTimestamp >= currentIndexInfo.timestamp) {
        // 目标在右半部分
        minIndex = index;
        index = minIndex + (maxIndex - minIndex) / 2;
    } else {
        // 目标在左半部分
        maxIndex = index;
        // 精调到±16条范围内,然后回退
    }
}
```

### 7.2 IndexInfoIterator (多文件迭代)

组合多个IndexInfoFileIterator,按时间范围跨文件遍历:
- 自动跳过无数据的索引文件
- 对无数据的秒返回 `IndexInfo(-1, -1, timestamp)`(占位符)

---

## 八、写入完整流程

以写入波形+测值数据为例:

```
1. FileManager.storeStreamData(device, iotMessage)
        ↓
2. 识别数据类型 → WaveDataBinaryProcess.convert() → MonitorRealTimeWaveData
   → MeasureDataBinaryProcess.convert() → MonitorRealTimeMeasureData
        ↓
3. 获取/创建对应QueueMonitorFile
   - getIndexQueueMonitorFile(timestamp)
   - getWaveQueueMonitorFile(timestamp)
   - getMeasureQueueMonitorFile(timestamp)
        ↓
4. 查找/创建合适MonitorFile
   - QueueMonitorFile.getLastMappedFile(device, offset, timestamp)
   - 若满 → tryCreateMappedFile() 创建新MappedByteBuffer文件
        ↓
5. 数据编码与写入
   - WaveDataBinaryProcess.encode() → protobuf bytes
   - MonitorFile.appendData(buffer.bytes, compress=true)
   → 自动更新writePosition、文件头
        ↓
6. 更新索引(type 0)
   - getIndexMonitorFileByTimestamp(device, timestamp)
   - 在索引文件中写入(waveOffset, measureOffset, timestamp)
        ↓
7. 同步WAR日志(若启用)
   - recordMonitorWarFile.appendMeasureWaveDataWar(device, monitorFile, dataOffset)
   → 写入16字节WAR条目
   → 更新WarStateFile的currentOffset
```

---

## 九、读取完整流程

按时间范围读取时序数据:

```
1. FileManager.openDataIterator(device, fileType, startTime, endTime)
        ↓
2. 获取对应RecordMonitorFile
        ↓
3. 按时间范围找到相关文件
   - QueueMonitorFile.findMappedFilesByTimespan(start, end)
        ↓
4. 创建迭代器
   - 索引: IndexInfoIterator(monitorFiles, startTimestamp, endTimestamp)
   - 数据: 直接遍历MonitorFileMonitorIterator
        ↓
5. 迭代读取数据
   - MonitorFileIterator.next() → MonitorFileReader → readBuffer() → 解压 → protobuf解析
   - IndexInfoIterator.next() → IndexInfo(waveOffset, measureOffset, timestamp)
        ↓
6. 数据组装
   - 通过IndexInfo中的offset在波形/测值文件中定位
   - 读取+解压+反序列化 → WaveData + MeasureData对
```

---

## 十、FileManager门面API

### 10.1 生命周期管理

| 方法 | 行为 |
|------|------|
| `open(device, recordId)` | 创建RecordMonitorFile,初始化元数据 |
| `replay(device, recordId)` | 从RocksDB恢复设备元数据 |
| `closeDevice(device)` | 关闭设备相关的所有文件 |
| `updateDeviceLastOnline(device)` | 更新设备在线时间戳 |
| `updateDeviceLastOffline(device)` | 更新设备离线时间戳 |
| `updateRecordFinish(device)` | 标记记录完成 |
| `deleteRecord(device, recordId)` | 删除指定记录的所有文件+WAR |

### 10.2 数据写入

| 方法 | 数据类型 | 压缩 |
|------|---------|------|
| `storeWaveDataAndMeasureData` | 波形+测值对 | 是(deflate) |
| `storeMonitorEventData` | 事件数据 | 是(deflate) |
| `storeMonitorMeasureManualData` | 手动测值 | 是(deflate) |

### 10.3 数据读取

| 方法 | 用途 |
|------|------|
| `openWaveDataAndMeasureDataIterator` | 按时间范围读取波形+测值对 |
| `openMonitorEventDataIterator` | 按时间范围读取事件 |
| `openMonitorMeasureManualDataIterator` | 按时间范围读取手动测值 |
| `openFileDataIterator` | 读取单个文件内的原始protobuf数据 |
| `openIndexIterator` | 遍历索引(IndexInfo流) |
| 返回: `Iterator<MonitorFile.MonitorFileMonitorIterator>` | 两层迭代: 文件迭代器→记录迭代器 |

### 10.4 WAR推送

| 方法 | 行为 |
|------|------|
| `openWarFileReader(device, startOffset, endOffset)` | 按WAR偏移范围读取变更日志 |
| `updatePushOffset(device, nodeId, offset)` | 更新推送进度 |
| `getPushOffset(device, nodeId)` | 查询推送进度 |
| `updatePushAckOffset(device, nodeId, offset)` | 更新确认进度 |
| `deleteRecordWar` | 删除WAR文件+WarStateFile |

### 10.5 内部线程(自动维护)

| 线程 | 间隔 | 行为 |
|------|------|------|
| `flush-monitorcare-data` | 每5秒 | flush所有QueueMonitorFile到磁盘 |
| `check-idle-monitorcare` | 每60秒 | 检查30分钟未使用的文件并关闭释放 |
| `clear-expired-data` | 每120秒 | 按dataExpiredDays清理过期文件 |

---

## 十一、RocksDB存储方案

### 11.1 Column Family设计

| CF名称 | Key | Value | 用途 |
|--------|-----|-------|------|
| `device_monitor_meta` | deviceCode:recordId | RecordMetaData(protobuf) | 设备元数据持久化 |
| `device_monitor_event` | deviceCode:recordId | DeviceMonitorEventPersistence | 事件状态存储 |
| `device_monitor_manual` | deviceCode:recordId | DeviceMonitorMeasureManualDataPersistence | 手动测值状态存储 |
| `device_file_push_meta` | deviceCode:recordId | DeviceFilePushData | 文件推送状态 |

### 11.2 Key编码

RocksDB使用复合Key:
```
Key = deviceCode + "::::" + recordId
```

---

## 十二、并发控制策略

| 对象 | 锁机制 | 说明 |
|------|--------|------|
| MonitorFile | `synchronized` | 文件写入互斥 |
| QueueMonitorFile | `synchronized` + `CopyOnWriteArrayList` | 文件队列线程安全 |
| RecordMonitorFile | `synchronized` | 记录级操作互斥 |
| WarStateFile | `ReentrantReadWriteLock` | WAR状态读写分离 |
| RocksDB | 线程安全(原生) | RocksDB自身保证 |
| BufferUtils | `ObjectPool<Buffer>` | 对象池复用,池内synchronized |
| FileManager | `ConcurrentHashMap` + `ConcurrentLinkedDeque` | 设备级并发控制 |

### 12.1 Buffer池

```java
ObjectPool<Buffer> monitorFileReadBufferPool;   // 读取缓冲池 (2048池大小, 10240容量)
ObjectPool<Buffer> monitorFileWriteBufferPool;  // 写入缓冲池 (2048池大小, 65535容量)
```

使用 `BufferUtils.getBuffer(size)` 获取,**必须**在finally中 `buffer.release()` 归还。

---

## 十三、数据压缩

### 13.1 压缩类型

| 数据类型 | 压缩 | 压缩级别 |
|---------|------|---------|
| 索引 | 否 | - |
| 波形 | 是 (deflate) | 7 |
| 测值 | 是 (deflate) | 7 |
| 事件 | 是 (deflate) | 7 |
| WAR | 否 | - |
| 手动测值 | 是 (deflate) | 7 |

### 13.2 压缩写入

```java
private void writeBuffer(final byte[] buffer, Deflater deflater, ...) {
    deflater.reset();
    deflater.setInput(buffer);
    deflater.finish();
    int size = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);
    mappedByteBuffer.putInt(size);  // 压缩后大小
    mappedByteBuffer.put(compressedBuffer, 0, size);
    writeSize += size + 4;
}
```

### 13.3 解压读取

```java
public int readBuffer(final int position, BufferUtils.Buffer buffer, Inflater inflater) {
    inflater.reset();
    int size = readSize(position);  // 读压缩后大小
    inflater.setInput(readBytes, 4, size);
    int length = inflater.inflate(buffer.getByteBuffer());
    return length;  // 解压后大小
}
```

---

## 十四、空闲回收与数据过期

### 14.1 空闲超时 (30分钟)

```java
updateLastUsedTimestamp(0):
    if (currentTime - lastUsedTimestamp >= 1800000ms (30min)) {
        return true;  // 触发关闭
    }
```

关闭流程: `flush() → mappedByteBuffer.force() → unmap() → close channels`

### 14.2 数据过期清理

```java
cleanUp(dataExpiredDays):
    遍历mappedFiles → 检查 fileMetadata.timestamp < expiredThreshold
    → monitorFile.delete() → 从队列移除
```

**RecordMonitorFile级清理**:
- `recordFinishTimestamp > 0` 且超时 → 删除所有文件+目录
- `lastOfflineTimestamp > 0` 且超时 → 删除所有文件+目录
- `recordStartTimestamp > 0` 但未超时 → 仅清理过期文件,保留目录

---

## 十五、关键限制

| 限制 | 说明 |
|------|------|
| ❌ 不支持更新 | 所有数据仅追加写入 |
| ❌ 不支持删除 | 仅支持过期自动清理,不支持单条删除 |
| ❌ 不支持事务 | 无跨文件事务保证 |
| ⚠️ MappedByteBuffer上限 | 受JVM内存限制,单文件最大2GB |
| ⚠️ 时间连续性 | 索引文件要求时间戳连续,跳跃时需填充空白 |
| ⚠️ 文件大小固定 | 每种类型文件大小预分配,无法动态调整 |
| ⚠️ 压缩不可变 | deflate压缩后的数据无法原地修改 |

---

## 十六、设计亮点总结

1. **按类型分段**: 6种数据类型隔离,互不阻塞
2. **内存映射IO**: MappedByteBuffer实现零拷贝读写
3. **二级索引**: 事件/手动测值的timestamp索引加速查找
4. **预写日志**: WAR机制支持数据推送/重放
5. **对象池复用**: Buffer池减少GC压力
6. **懒加载/懒关闭**: 按需创建,30分钟空闲自动关闭
7. **压缩**: deflate压缩减少磁盘占用(波形/测值/事件)
8. **RocksDB混合**: 元数据+状态用RocksDB,大数据用文件
