# MonitorCare Orbit - 时序数据存储系统详细分析

## 1. 系统概述

本项目实现了一个基于文件系统的流式时序数据存储读写系统，专为医疗监护设备数据设计。系统采用内存映射文件(MappedByteBuffer)技术实现高效I/O操作，支持按时间范围读取，不支持更新和删除操作。

### 1.1 核心设计原则

- **追加写入(Append-Only)**：所有数据只支持追加写入，不支持更新和删除
- **时间分段存储**：数据按时间戳分段存储在不同文件中
- **索引分离**：索引数据与实际数据分离存储，实现高效时间范围查询
- **内存映射**：使用MappedByteBuffer实现零拷贝读写
- **压缩存储**：波形和测量数据支持Deflate压缩

## 2. 架构层次

```
FileManager (顶层管理器)
    │
    ├── RecordMonitorFile (记录会话管理)
    │       │
    │       ├── QueueMonitorFile (类型0: Index索引队列)
    │       │       └── MonitorFile[] (索引文件列表)
    │       │
    │       ├── QueueMonitorFile (类型1: Wave波形数据队列)
    │       │       └── MonitorFile[] (波形文件列表)
    │       │
    │       ├── QueueMonitorFile (类型2: Measure测值数据队列)
    │       │       └── MonitorFile[] (测值文件列表)
    │       │
    │       ├── QueueMonitorFile (类型3: Event事件数据队列)
    │       │       └── MonitorFile[] (事件文件列表)
    │       │
    │       ├── QueueMonitorFile (类型5: ManualMeasure手动测量队列)
    │       │       └── MonitorFile[] (手动测量文件列表)
    │       │
    │       └── RecordMonitorWarFile (WAR写前记录)
    │               ├── WarStateFile (状态追踪)
    │               └── QueueMonitorFile (类型4: WAR队列)
    │
    └── RocksPersistence (RocksDB持久化)
            └── 存储Event和ManualMeasure的最新状态
```

## 3. 数据类型定义

| 类型编号 | 类型名称 | 文件大小 | 存储内容 | 是否压缩 |
|---------|---------|---------|---------|---------|
| 0 | Index | 16MB | 索引数据(24字节/秒) | 否 |
| 1 | Wave | 64MB | 波形数据 | 是(Deflate level=7) |
| 2 | Measure | 32MB | 测量数值数据 | 是(Deflate level=7) |
| 3 | Event | 8MB | 事件数据 | 是(Deflate level=7) |
| 4 | War | 16MB | 写前记录(同步用) | 否 |
| 5 | ManualMeasure | 8MB | 手动测量数据 | 是(Deflate level=7) |

## 4. 核心类详细分析

### 4.1 FileManager

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.FileManager`

**职责**: 全局管理器，管理所有RecordMonitorFile实例，协调写入和读取操作。

**关键属性**:
```java
private static final ConcurrentHashMap<Long, RecordMonitorFile> recordMonitorFiles;
private final String dataDir;                    // 数据存储根目录
private final Integer dataExpiredDays;           // 数据过期天数
private final boolean dataWarEnabled;            // WAR功能开关
private final RocksPersistence rocksPersistence; // RocksDB持久化
```

**核心方法**:

| 方法 | 功能 |
|-----|------|
| `getRecordMonitorFile(recordId)` | 获取或创建指定记录的MonitorFile |
| `writeMeasureWaveData()` | 写入波形和测量数据 |
| `writeEventData()` | 写入事件数据 |
| `writeMeasureManualData()` | 写入手动测量数据 |
| `read(FileRequest, StreamObserver)` | 按时间范围读取数据 |
| `cleanUpExpiredMonitorFiles()` | 清理过期数据文件 |

### 4.2 RecordMonitorFile

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.core.RecordMonitorFile`

**职责**: 单个监护记录会话的文件管理，包含该记录的所有数据类型队列。

**文件存储结构**:
```
{dataDir}/record/{recordId}/
    ├── meta.pbb          # 记录元数据(Protobuf)
    ├── device.pbb        # 设备信息(Protobuf)
    ├── patient.pbb       # 患者信息(Protobuf)
    ├── war.pbb           # WAR状态文件
    ├── 0/                # Index索引目录
    │   └── {timestamp}   # 索引文件(文件名=时间戳)
    ├── 1/                # Wave波形目录
    │   └── {offset}      # 波形文件(文件名=偏移量)
    ├── 2/                # Measure测值目录
    │   └── {offset}      # 测值文件
    ├── 3/                # Event事件目录
    │   └── {timestamp}   # 事件文件(文件名=时间戳)
    ├── 5/                # ManualMeasure目录
    │   └── {timestamp}   # 手动测量文件
    └── 4/                # War目录
        └── {offset}      # WAR文件
```

### 4.3 QueueMonitorFile

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.core.QueueMonitorFile`

**职责**: 管理特定数据类型的文件队列，处理文件创建、查找、清理。

**关键属性**:
```java
private final int fileType;                    // 数据类型(0-5)
private final int mappedFileSize;              // 单文件大小限制
private long startTimespan = 0L;                // 数据起始时间
private long endTimespan = 0L;                  // 数据结束时间
private final CopyOnWriteArrayList<MonitorFile> mappedFiles; // 文件列表
```

**核心逻辑**:

1. **文件命名规则**:
   - Index/Event/ManualMeasure: 文件名 = 20位数字的时间戳
   - Wave/Measure/War: 文件名 = 20位数字的偏移量

2. **文件查找**:
   - `findMappedFilesByTimespan(start, end)` - 按时间范围查找
   - `findMappedFileByOffset(offset)` - 按偏移量查找
   - `getLastMappedFile()` - 获取最新文件

3. **文件创建**:
   - `tryCreateMappedFile(device, offset, timestamp)` - 创建新文件
   - 自动按时间戳或偏移量命名

### 4.4 MonitorFile

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.core.MonitorFile`

**职责**: 单个内存映射文件的管理，处理具体读写操作。

**文件格式**:
```
┌──────────────────────────────────────────────────────┐
│ Header (文件头)                                       │
│ ┌─────────┬───────────┬───────────┬─────────────────┐│
│ │ Magic   │ MetaSize  │ Metadata  │ WrotePosition   ││
│ │ "NMPB"  │ (4 bytes) │ (Protobuf)│ (4 bytes)       ││
│ │ (4B)    │           │           │                 ││
│ └─────────┴───────────┴───────────┴─────────────────┘│
│ State Section (状态区)                                │
│ ┌─────────────────┬─────────────────┐               │
│ │ WrotePosition   │ DataSize        │               │
│ │ (4 bytes)       │ (4 bytes)       │               │
│ └─────────────────┴─────────────────┘               │
│ Index Section (索引区，可选)                          │
│ ┌─────────────────┬─────────────────┐               │
│ │ Timestamp       │ Position        │ × N           │
│ │ (8 bytes)       │ (8 bytes)       │               │
│ └─────────────────┴─────────────────┘               │
│ Data Section (数据区)                                 │
│ ┌─────────────────┬─────────────────┐               │
│ │ Size            │ Data            │ × N           │
│ │ (4 bytes)       │ (variable)      │               │
│ └─────────────────┴─────────────────┘               │
└──────────────────────────────────────────────────────┘
```

**关键方法**:

| 方法 | 功能 |
|-----|------|
| `appendData(data, needSize)` | 追加数据(可选是否记录size) |
| `writeData(position, data)` | 在指定位置写入数据 |
| `updateTimeIndex(timestamp, offset)` | 更新时间索引 |
| `findIndexPositionByTimestamp(timestamp)` | 按时间查找索引位置 |
| `flush()` / `close()` | 刷新/关闭文件 |

**压缩处理**:
```java
// 压缩写入
private int appendCompressData(byte[] data, int offset, int length) {
    deflater.setInput(data, offset, length);
    deflater.finish();
    while (!deflater.finished()) {
        int i = deflater.deflate(buffer);
        mappedByteBuffer.put(buffer, 0, i);
        size += i;
    }
    mappedByteBuffer.putInt(lastOffset, size); // 记录压缩后大小
}
```

### 4.5 IndexInfo & Iterator

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.index.*`

**职责**: 索引信息结构和遍历迭代器。

**IndexInfo结构**:
```java
public class IndexInfo {
    private long waveOffset;    // 波形数据偏移量
    private long measureOffset; // 测量数据偏移量
    private long timestamp;     // 时间戳(秒)
}
```

**IndexInfoIterator遍历逻辑**:
1. 支持按时间范围[startTimestamp, endTimestamp]遍历
2. 使用二分查找快速定位起始位置
3. 每秒生成一个IndexInfo(无数据时返回空IndexInfo)

**IndexInfoFileIterator核心算法**:
```java
// 二分查找定位起始时间戳
while (true) {
    index++;
    currentIndexInfo = update(byteBuffer.getLong(), byteBuffer.getLong(), byteBuffer.getLong());
    if (startTimestamp >= currentIndexInfo.getTimestamp()) {
        if (startTimestamp - currentIndexInfo.getTimestamp() <= 16) break;
        minIndex = index;
        index = minIndex + (maxIndex - minIndex) / 2;
    } else {
        maxIndex = index;
        index = minIndex + (maxIndex - minIndex) / 2;
    }
}
```

## 5. 写入流程详解

### 5.1 波形和测量数据写入

**入口方法**: `FileManager.writeMeasureWaveData(IotMessage, IDevice)`

**流程**:
```
1. 获取RecordMonitorFile
2. 转换数据格式
   ├── WaveDataBinaryProcess.convert(message) → MonitorRealTimeWaveData
   └── MeasureDataBinaryProcess.convert(message) → MonitorRealTimeWaveData
3. 调用内部writeMeasureWaveData方法
   │
   ├── 获取Index QueueMonitorFile
   │   └── 计算timestamp对应的IndexMonitorFile位置
   │
   ├── 处理Wave数据 (processData)
   │   ├── 获取Wave QueueMonitorFile
   │   ├── 获取/创建最后一个MonitorFile
   │   ├── appendData(压缩后的波形数据)
   │   └── 返回全局偏移量 waveOffset
   │
   ├── 处理Measure数据 (processData)
   │   ├── 获取Measure QueueMonitorFile
   │   ├── 获取/创建最后一个MonitorFile
   │   ├── appendData(压缩后的测值数据)
   │   └── 返回全局偏移量 measureOffset
   │
   └── 处理Index数据 (handleIndexData)
       ├── 构建24字节索引记录
       │   [waveOffset(8) + measureOffset(8) + timestamp(8)]
       ├── 追加到IndexMonitorFile
       └── 更新QueueMonitorFile.endTimespan
```

**关键代码**:
```java
// FileManager.java - 写入流程核心
public void writeMeasureWaveData(IDevice device, RecordMonitorFile recordMonitorFile, 
    long timestamp, MonitorRealTimeMeasureData measure, MonitorRealTimeWaveData wave) {
    
    QueueMonitorFile queueMonitorFile = recordMonitorFile.getIndexQueueMonitorFile(timestamp, device);
    MonitorFile indexMonitorFile;
    
    // 判断timestamp位置
    if (queueMonitorFile.getEndTimespan() == 0) {
        // 新文件，创建第一个index文件
        indexMonitorFile = queueMonitorFile.getLastMappedFile(device, 0, timestamp);
    } else if (timestamp <= queueMonitorFile.getEndTimespan()) {
        // 回填历史数据
        indexMonitorFile = queueMonitorFile.getIndexMonitorFileByTimestamp(device, timestamp);
        writePosition = indexMonitorFile.getDataPosition() + (timestamp - fileFromOffset) * 24;
    } else if (timestamp - queueMonitorFile.getEndTimespan() > 1) {
        // 大间隔跳跃，需要填充空白索引
        indexMonitorFile = queueMonitorFile.fillIndexMonitorFileByTimestamp(device, timestamp);
    } else {
        // 正常追加
        indexMonitorFile = queueMonitorFile.getLastMappedFile(device, 0, timestamp);
    }
    
    // 写入wave和measure数据
    long waveOffset = processData(device, recordMonitorFile, timestamp, 
        WaveDataBinaryProcess.encode(wave), 0x01);
    long measureOffset = processData(device, recordMonitorFile, timestamp,
        MeasureDataBinaryProcess.encode(measure), 0x02);
    
    // 写入索引
    handleIndexData(queueMonitorFile, indexMonitorFile, waveOffset, measureOffset, timestamp);
}
```

### 5.2 事件数据写入

**入口方法**: `FileManager.writeEventData(List<MonitorEventData>, dataType, IDevice)`

**流程**:
```
1. 按时间戳排序事件列表
2. 每5分钟分组处理
3. 对每组:
   ├── 构建MonitorEventRealTimeData (Protobuf)
   ├── 获取Event QueueMonitorFile
   ├── 判断是否需要更新时间索引
   │   ├── endTimespan - timestamp >= 900秒: 使用已存在索引
   │   └── 否则: 创建新索引
   ├── appendData(压缩后的事件数据)
   ├── updateTimeIndex(timestamp, fileOffset)
   └── 更新RocksDB持久化(用于记录最新状态)
```

### 5.3 手动测量数据写入

与事件数据写入类似，使用fileType=5的队列。

## 6. 读取流程详解

### 6.1 按时间范围读取

**入口方法**: `FileManager.read(FileRequest, StreamObserver)`

**请求参数**:
```java
FileRequest {
    long recordId;          // 记录ID
    int fileType;           // 数据类型(0-5)
    long startTimespan;     // 起始时间戳
    long duration;          // 持续时间(秒)
    int seekType;           // 定位类型(0:绝对, 1:相对末尾, 2:自动调整)
    int compressType;       // 响应压缩类型(0:不压缩, 1:压缩)
    int samplingPeriod;     // 采样周期(秒)
}
```

**读取流程**:
```
1. 处理seekType定位
   ├── seekType=1: 相对末尾定位
   ├── seekType=2: 自动调整边界
   └── seekType=0: 直接使用startTimespan

2. 构建DownloadMetaData响应头

3. 根据fileType分发处理
   │
   ├── fileType=0 (Index):
   │   ├── findMappedFilesByTimespan(start, end)
   │   ├── 创建IndexInfoIterator
   │   └── 遍历输出每秒的索引状态(byte)
   │       ├── 有wave数据: byte |= 0x01
   │       └── 有measure数据: byte |= 0x02
   │
   ├── fileType=1 (Wave):
   │   ├── 创建IndexInfoIterator遍历索引
   │   ├── 对每个有waveOffset的索引:
   │   ├── 计算文件索引 = waveOffset / WAVE_FILE_SIZE
   │   ├── 读取并解压缩波形数据
   │   └── 输出到StreamObserver
   │
   ├── fileType=2 (Measure):
   │   └── 同Wave流程，读取measureOffset指向的数据
   │
   ├── fileType=3 (Event):
   │   ├── findMappedFilesByTimespan(start, end)
   │   ├── 使用自索引定位数据范围
   │   └── 遍历读取每个事件数据
   │
   └── fileType=5 (ManualMeasure):
       └── 同Event流程
```

**响应格式**:
```
┌──────────────────────────────────────────────┐
│ DownloadMetaData (Protobuf)                  │
│ ├── version, recordId, timestamp            │
│ ├── startTimestamp, endTimestamp            │
│ ├── recordType, compressType, compressLevel │
│ └── samplingPeriod                           │
├──────────────────────────────────────────────┤
│ Data Records                                 │
│ ┌────────────┬─────────────────────────────┐ │
│ │ Length     │ Data (Protobuf)             │ × N│
│ │ (4 bytes)  │ (variable)                  │   │
│ └────────────┴─────────────────────────────┘   │
│ Length=0 表示该秒无数据                       │
└──────────────────────────────────────────────┘
```

### 6.2 时间索引查找优化

**MonitorFile.findIndexPositionByTimestamp**使用跳跃查找优化:
```java
public int findIndexPositionByTimestamp(long timestamp) {
    int startWrotePosition = getIndexPosition();
    
    // 跳跃查找 - 每次跳跃64字节(4个索引项)
    while (startWrotePosition + 64 < endWrotePosition) {
        indexTimestamp = mappedByteBuffer.getLong(startWrotePosition + 64);
        if (indexTimestamp > 0 && indexTimestamp < timestamp) {
            startWrotePosition += 64;
            // 动态增大跳跃步长: 256 -> 1024 -> 4096...
            int stepSize = 256;
            while (stepSize < maxStepSize && startWrotePosition + stepSize < endWrotePosition) {
                if (mappedByteBuffer.getLong(startWrotePosition + stepSize) < timestamp) {
                    startWrotePosition += stepSize;
                    stepSize *= 4;
                } else {
                    maxStepSize = stepSize;
                    break;
                }
            }
        } else {
            break;
        }
    }
    
    // 精确线性查找
    while (startWrotePosition < endWrotePosition) {
        indexTimestamp = mappedByteBuffer.getLong(startWrotePosition);
        if (indexTimestamp == 0 || indexTimestamp > timestamp) return startWrotePosition - 16;
        if (indexTimestamp == timestamp) return startWrotePosition;
        startWrotePosition += 16;
    }
}
```

## 7. WAR(Write-Ahead Record)系统

### 7.1 设计目的

WAR系统用于数据同步和推送，记录所有写入操作的顺序和位置，支持多节点间的数据同步。

### 7.2 核心组件

**RecordMonitorWarFile**:
- 管理WAR队列和状态文件
- 记录每种数据类型的写入位置

**WarStateFile**:
- 追踪当前写入偏移量(currentOffset)
- 追踪各节点的推送偏移量(pushOffset)
- 追踪各节点的确认偏移量(pushAckOffset)

**WarType枚举**:
```java
public enum WarType {
    None(0),
    RecordMetaData(1),    // 记录元数据变更
    RecordDeviceData(2),  // 设备信息变更
    RecordPatientData(3), // 患者信息变更
    MeasureWaveData(4),   // 波形测值数据
    EventData(5),         // 事件数据
    MeasureManualData(6); // 手动测量数据
}
```

### 7.3 WAR记录格式

每条WAR记录16字节:
```
┌────────────┬────────────────┬────────────────┐
│ WarType    │ FileStartOffset│ FileDataOffset │
│ (4 bytes)  │ (8 bytes)      │ (4 bytes)      │
└────────────┴────────────────┴────────────────┘
```

### 7.4 数据同步流程

```
写入端:
1. 写入实际数据文件
2. 追加WAR记录(warType + fileOffset + dataOffset)
3. 更新WarStateFile.currentOffset
4. 推送数据到目标节点

接收端:
1. 读取WAR记录(warOffset范围)
2. 根据WarType加载对应数据
3. 处理数据
4. 更新WarStateFile.pushAckOffset
```

## 8. 数据过期清理机制

### 8.1 清理触发条件

```java
// RecordMonitorFile.cleanUp()
boolean isFinish = dataExpiredDays > 0 
    && metaData.getRecordFinishTimestamp() > 0
    && System.currentTimeMillis() - metaData.getRecordFinishTimestamp() 
        >= dataExpiredDays * 86400000L;

boolean isOfflineExpired = dataExpiredDays > 0 
    && metaData.getLastOfflineTimestamp() > 0
    && System.currentTimeMillis() - metaData.getLastOfflineTimestamp() 
        >= dataExpiredDays * 86400000L;
```

### 8.2 清理流程

```
1. 遍历所有RecordMonitorFile
2. 检查过期条件
   ├── 记录完成时间超过过期天数
   └── 最后离线时间超过过期天数
3. 执行清理
   ├── 已完成/已过期: 删除整个记录目录
   └── 未完成但部分过期: 清理早期数据文件
```

### 8.3 文件级清理

```java
// QueueMonitorFile.cleanUp()
for (int i = 0; i < monitorFiles.size() - 1; i++) {
    if (System.currentTimeMillis() - fileMetadata.getTimestamp() < expiredDays * 86400000L) break;
    if (nextFileMetadata != null && 
        System.currentTimeMillis() - nextFileMetadata.getTimestamp() < expiredDays * 86400000L) break;
    monitorFile.delete();
}
```

## 9. RocksDB持久化

**位置**: `com.nalong.monitorcare.orbit.handler.filesystem.RocksPersistence`

**存储内容**:
- DeviceMonitorMeasureManualDataPersistence: 手动测量数据最新状态
- DeviceMonitorEventPersistence: 事件数据最新状态

**用途**:
- 用于跨记录查询最新手动测量/事件状态
- 记录完成后自动清理

**Key格式**:
```
device:monitor:measure:manual:{deviceCode}:{recordId}
device:monitor:event:{deviceCode}:{recordId}
```

## 10. 性能优化设计

### 10.1 内存映射(MappedByteBuffer)

- 零拷贝读写，避免系统调用开销
- 操作系统自动管理页面缓存
- 写入时使用`force()`强制刷盘

### 10.2 压缩存储

- 波形/测值/事件数据使用Deflate压缩(level=7)
- 平均压缩率约50%-70%
- 读取时动态解压缩

### 10.3 时间索引优化

- 每秒一个索引项(24字节)
- 跳跃查找减少比较次数
- 二分查找快速定位

### 10.4 文件预分配

- 每个文件固定大小(避免动态扩展开销)
- 写满后创建新文件

### 10.5 并发控制

- ReentrantReadWriteLock读写锁
- CopyOnWriteArrayList文件列表
- ConcurrentHashMap缓存

## 11. 数据流转完整示例

### 11.1 波形数据写入示例

```java
// 1. IoT设备推送消息
IotMessage message = device.receiveWaveData();

// 2. FileManager处理
FileManager fileManager = new FileManager(dataDir, expiredDays, warEnabled);
fileManager.writeMeasureWaveData(message, device);

// 3. 内部处理流程
RecordMonitorFile recordFile = fileManager.getRecordMonitorFile(device.getRecordId());
long timestamp = message.getTimestamp() / 1000;

// 4. 获取索引队列
QueueMonitorFile indexQueue = recordFile.getIndexQueueMonitorFile(timestamp, device);

// 5. 写入波形数据
QueueMonitorFile waveQueue = recordFile.getWaveQueueMonitorFile(timestamp, device);
MonitorFile waveFile = waveQueue.getLastMappedFile(device, 0, timestamp);
long waveOffset = waveFile.appendData(compressedWaveData, true);

// 6. 写入测值数据
QueueMonitorFile measureQueue = recordFile.getMeasureQueueMonitorFile(timestamp, device);
MonitorFile measureFile = measureQueue.getLastMappedFile(device, 0, timestamp);
long measureOffset = measureFile.appendData(compressedMeasureData, true);

// 7. 写入索引
MonitorFile indexFile = indexQueue.getLastMappedFile(device, 0, timestamp);
ByteBuffer indexBuffer = ByteBuffer.allocate(24);
indexBuffer.putLong(waveOffset);
indexBuffer.putLong(measureOffset);
indexBuffer.putLong(timestamp);
indexFile.appendData(indexBuffer.array(), false);

// 8. 记录WAR
RecordMonitorWarFile warFile = recordFile.getRecordMonitorWarFile();
warFile.appendMeasureWaveDataWar(device, indexFile, (int)fileOffset);
```

### 11.2 时间范围读取示例

```java
// 1. 构建请求
FileRequest request = FileRequest.newBuilder()
    .setRecordId(123456)
    .setFileType(1)  // Wave
    .setStartTimespan(System.currentTimeMillis() / 1000 - 3600)
    .setDuration(3600)
    .setSeekType(0)
    .setCompressType(0)
    .setSamplingPeriod(1)
    .build();

// 2. FileManager处理
StreamObserver<FileResponse> observer = ...;
fileManager.read(request, observer);

// 3. 内部读取流程
RecordMonitorFile recordFile = fileManager.getRecordMonitorFile(request.getRecordId());
QueueMonitorFile indexQueue = recordFile.getQueueMonitorFile(0, 0);
QueueMonitorFile waveQueue = recordFile.getWaveQueueMonitorFile(0);

// 4. 查找索引文件
List<MonitorFile> indexFiles = indexQueue.findMappedFilesByTimespan(start, end);

// 5. 遍历索引
IndexInfoIterator iterator = new IndexInfoIterator(indexFiles, start, end);
while (iterator.hasNext()) {
    IndexInfo info = iterator.next();
    if (info.getWaveOffset() > 0) {
        // 6. 定位波形文件
        int fileIndex = info.getWaveOffset() / WAVE_FILE_SIZE;
        MonitorFile waveFile = waveQueue.getMappedFiles().get(fileIndex);
        
        // 7. 读取解压
        long pos = info.getWaveOffset() % WAVE_FILE_SIZE;
        byte[] data = waveFile.readBuffer(pos, inflater);
        
        // 8. 输出
        observer.onNext(FileResponse.newBuilder()
            .setData(ByteString.copyFrom(data))
            .setSize(totalBytes)
            .build());
    }
}
```

## 12. 局限性与设计约束

### 12.1 不支持的操作

| 操作 | 原因 |
|-----|------|
| 更新数据 | 追加写入设计，索引已固化 |
| 删除数据 | 会破坏索引连续性 |
| 随机写入 | 索引文件按时间顺序排列 |

### 12.2 设计约束

1. **时间戳必须递增**: 索引文件按时间顺序存储
2. **不允许时间回退**: 回退需要填充空白索引
3. **单记录单设备**: 每个RecordId绑定单个设备
4. **文件大小固定**: 创建后不动态扩展

### 12.3 性能边界

- **最大索引密度**: 每秒24字节，16MB文件约可存储~600秒数据
- **最大波形容量**: 64MB文件，压缩后约可存储数千秒波形
- **索引查找复杂度**: O(n/跳跃步长) ≈ O(log n)

## 13. 总结

本系统是一个专为医疗监护场景设计的时序数据存储系统，核心特点:

1. **追加写入架构**: 简化写入逻辑，避免锁竞争
2. **索引分离设计**: 实现高效时间范围查询
3. **内存映射技术**: 最大化I/O性能
4. **压缩存储**: 节省存储空间
5. **WAR同步机制**: 支持多节点数据同步
6. **自动过期清理**: 避免数据无限增长

系统通过精心设计的文件组织结构、索引机制和内存管理，实现了高吞吐量的流式数据写入和高效的时间范围查询能力。