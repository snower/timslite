/* eslint-disable */

export interface StoreConfig {
  flushIntervalMs?: number
  idleTimeoutMs?: number
  dataSegmentSize?: number | bigint
  indexSegmentSize?: number | bigint
  initialDataSegmentSize?: number | bigint
  initialIndexSegmentSize?: number | bigint
  compressLevel?: number
  compressType?: 0 | 1
  cacheMaxMemory?: number | bigint
  cacheIdleTimeoutMs?: number
  retentionCheckHour?: number
  enableBackgroundThread?: boolean
  enableJournal?: boolean
  readOnly?: boolean | null
}

export interface CreateDatasetOptions {
  dataSegmentSize?: number | bigint
  indexSegmentSize?: number | bigint
  initialDataSegmentSize?: number | bigint
  initialIndexSegmentSize?: number | bigint
  compressLevel?: number
  compressType?: 0 | 1
  indexContinuous?: boolean
  retentionWindow?: number | bigint
  enableJournal?: boolean
}

export interface QueueConsumerOptions {
  runningExpiredSeconds?: number
  maxRetryCount?: number
}

export interface QueueConsumerInfo {
  groupName: string
  runningExpiredSeconds: number
  maxRetryCount: number
}

export interface QueueConsumerPendingEntry {
  timestamp: bigint
  startTime: bigint
  status: number
  retryCount: number
}

export interface QueueConsumerState {
  processedTs: bigint
  pendingEntries: QueueConsumerPendingEntry[]
}

export interface QueueConsumerInspectResult {
  info: QueueConsumerInfo
  state: QueueConsumerState
}

export interface DataSetInfo {
  name: string
  datasetType: string
  baseDir: string
  identifier: bigint
  dataSize: bigint
  indexSize: bigint
  initialDataSize: bigint
  initialIndexSize: bigint
  compressType: number
  compressLevel: number
  indexContinuous: number
  retentionWindow: bigint
  enableJournal: boolean
  createTime: bigint
}

export interface DataSetState {
  latestWrittenTimestamp: bigint | null
  openDataSegments: number
  dataSegments: number
  totalRecordCount: bigint
  totalDataSize: bigint
  totalUncompressedSize: bigint
  totalInvalidRecordCount: bigint
  minTimestamp: bigint | null
  maxTimestamp: bigint | null
  openIndexSegments: number
  indexSegments: number
  pendingIndexEntries: number
  baseTimestamp: bigint | null
  readOnly: boolean
  hasBlockCache: boolean
  hasJournal: boolean
  hasQueue: boolean
  queueConsumerGroups: number
}

export interface DataSetInspectResult {
  info: DataSetInfo
  state: DataSetState
}

export interface TickResult {
  executedTasks: number
  nextDelayMs: number
}

export interface JournalIndexInfo {
  timestamp: bigint
  blockOffset: bigint
  inBlockOffset: number
}

export class Store {
  static open(dataDir: string, config?: StoreConfig): Store
  close(): void
  createDataset(name: string, datasetType: string, options?: CreateDatasetOptions): void
  openDataset(name: string, datasetType: string): Dataset
  openDatasetByIdentifier(identifier: number | bigint): Dataset
  dropDataset(name: string, datasetType: string): void
  openQueue(dataset: Dataset): Queue
  openJournalQueue(): JournalQueue
  journalLatestSequence(): bigint | null
  journalRead(sequence: number | bigint): [bigint, Buffer] | null
  journalQuery(startSequence: number | bigint, endSequence: number | bigint): Array<[bigint, Buffer]>
  readJournalSourceRecord(identifier: number | bigint, indexInfo: JournalIndexInfo): [bigint, Buffer]
  tickBackgroundTasks(): TickResult
  nextBackgroundDelay(): number
  getDatasetNames(): string[]
  getDatasetTypes(name: string): string[]
  inspectDataset(name: string, datasetType: string): DataSetInspectResult
  readonly closed: boolean
  readonly readOnly: boolean
}

export class Dataset {
  write(timestamp: number | bigint, data: Buffer | Uint8Array): void
  append(timestamp: number | bigint, data: Buffer | Uint8Array): void
  delete(timestamp: number | bigint): void
  read(timestamp: number | bigint): [bigint, Buffer] | null
  readLatest(): [bigint, Buffer] | null
  readExist(timestamp: number | bigint): boolean
  readLength(timestamp: number | bigint): number | null
  query(startTs: number | bigint, endTs: number | bigint): QueryIterator
  queryAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, Buffer]>
  queryExist(startTs: number | bigint, endTs: number | bigint): Buffer
  queryLength(startTs: number | bigint, endTs: number | bigint): QueryLengthIterator
  queryLengthAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, number]>
  flush(): void
  close(): void
  inspect(): DataSetInspectResult
  readonly id: bigint
  readonly identifier: bigint
  readonly dataDir: string
  readonly latestTimestamp: bigint | null
  readonly closed: boolean
}

export class QueryIterator implements Iterable<[bigint, Buffer]> {
  [Symbol.iterator](): QueryIterator
  next(): IteratorResult<[bigint, Buffer]>
  reverse(): this
  skip(count: number): this
  collectAll(): Array<[bigint, Buffer]>
  collectTake(count: number): Array<[bigint, Buffer]>
}

export class QueryLengthIterator implements Iterable<[bigint, number]> {
  [Symbol.iterator](): QueryLengthIterator
  next(): IteratorResult<[bigint, number]>
  reverse(): this
  skip(count: number): this
  collectAll(): Array<[bigint, number]>
  collectTake(count: number): Array<[bigint, number]>
}

export class Queue {
  push(data: Buffer | Uint8Array): bigint
  openConsumer(groupName: string, options?: QueueConsumerOptions): QueueConsumer
  getConsumerGroupNames(): string[]
  dropConsumer(groupName: string): void
  close(): void
}

export class QueueConsumer {
  poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>
  pollSync(timeoutMs?: number): [bigint, Buffer] | null
  ack(timestamp: number | bigint): void
  flush(): void
  close(): void
  inspect(): QueueConsumerInspectResult
  pollCallback(callback: (() => void) | null): void
}

export class JournalQueue {
  openConsumer(groupName: string, options?: QueueConsumerOptions): JournalQueueConsumer
  close(): void
}

export class JournalQueueConsumer {
  poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>
  pollSync(timeoutMs?: number): [bigint, Buffer] | null
  ack(sequence: number | bigint): void
  pollCallback(callback: (() => void) | null): void
}

export function version(): string
