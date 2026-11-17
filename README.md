# qlog

有界内存、异步批量写、按分钟归档的 Go 日志库。Go 1.26.11+。

默认尽力写入：磁盘满、I/O 错误、权限错误、同步或封存失败都不会终止 Writer。写不出的记录计为失败并输出 ERROR 日志，磁盘恢复后使用新分段继续写入，无需重启服务。队列满时丢弃新记录，业务调用不等待磁盘，也不收到 I/O 错误。

```text
BaseDir=/data, Name=request
/data/request/request_202611122350_<writer-id>_000001.log
/data/request/request_202611122351_<writer-id>_000002.log
/data/request/request_202611122352_<writer-id>_000003.log.active
```

运行时使用固定的 `BaseDir/Name`，每次启动生成随机 Writer ID，每次轮转使用递增分段号，独占创建新文件，不追加已有文件。分段号在每次**尝试**打开新文件时递增（包括打开失败的情况），持续 I/O 故障后文件序号可能出现跳跃，但文件名长度仍在限制内。每个目录只允许一个活动 Writer，包括跨进程；调用方负责保证独占。一个进程可为多个不同目录分别创建 Writer。

## 接入

模块路径为 `github.com/yesgs/qlog`，包名为 `qlog`。本地集成可在业务模块中使用 `replace github.com/yesgs/qlog => /path/to/qlog`。

```go
w, err := qlog.New(qlog.Config{
    BaseDir: "/data",
    Name:    "request",
})
if err != nil {
    return err
}

// 尽力提交：返回后可以复用 data。队列满或后台写失败时可能丢弃。
if _, err := w.Write(data); err != nil {
    // 这里只返回 ErrClosed / ErrRecordTooLarge 等调用错误。
    // 磁盘错误和队列满由库记录 ERROR 日志及失败计数。
    return err
}

// 退出：先停止上游生产，再等待队列处理完、关闭文件。
// I/O 失败仍由 ERROR 日志和 Stats 报告；这里只返回等待超时/取消错误。
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
return w.Shutdown(ctx)
```

`Write` 不等待磁盘或队列空位；队列满时直接丢弃该记录并返回 `len(data), nil`。并发生产者会有短暂互斥锁竞争，它不是 lock-free 接口。需要随业务 context 停止生产时，由调用方在提交前检查取消状态。无需为每条日志创建 goroutine。

**`Write` 返回 `nil` 只表示库处理过这条记录，不表示已入队或已落盘**；`log.Logger`、`slog`、`io.Copy` 等标准库组合器会把它当作写入成功。需要感知丢弃时改用 `TryWrite`：它返回 `(n, Admission, error)`，用 `AdmissionQueued` / `AdmissionDropped` / `AdmissionRejected` 区分入队、队列满丢弃和返回错误的拒绝；丢弃时 `n` 仍是原始输入长度且 `error` 为 `nil`。

`New` 只校验配置，目录在后台首次打开分段时创建。启动时磁盘不可写也能创建 Writer，后续日志触发恢复尝试。`Name` 必须是有效 UTF-8，最多 177 字节，为完整分段文件名预留空间；不能包含路径分隔符、Windows 保留字符/设备名、控制字符或末尾空格/点。配置不合法仍会返回错误。

每次调用表示**一条日志**，内容中的 LF 原样保留；仅将末尾一个 LF（或 CRLF）规范成 LF，没有末尾 LF 时会补一个。空数据写成空行。记录数按调用次数统计，一条记录可以跨多行，无法仅凭文件中的换行还原记录边界。`Write` 返回值按 `io.Writer` 约定为原始输入长度，metrics 大小按最终写入长度计算，包含 LF。普通多行 `io.Copy` 不适用这个接口。

文件取**成功入队时刻**的年月日时分，队列积压跨分钟也保留原目标分钟。默认 `time.Local`，可以配置固定时区或 `time.UTC`。时钟回拨、夏令时重复分钟或同分钟重启时创建新分段，已经封存的文件永不重开。同一分钟可以有多个文件，文件名中的 ID 和分段号用于区分。空闲分钟不创建文件。

## 安全转移与归档

写入中的文件以 `.log.active` 结尾。对应记录写完、`fsync` 和 `Close` 均成功后，后台才将其原子发布为 `.log`。转移程序**只处理 `*.log`，忽略所有 `.active` 文件**；不能按文件名时间、修改时间或大小稳定来推断文件已经写完。`WrittenRecords` 增长也不表示文件已经封存。

切换入队分钟时封存上一分段。没有新日志时，独立的分钟定时器也会冲刷待写批次并封存旧分钟文件，不必等待下一条日志或较长的 `FlushInterval`。有积压时先处理对应记录，不能承诺跨分钟瞬间就完成封存。正常 `Shutdown` 会封存最后一个分段。

封存通过同目录硬链接原子发布已关闭的文件，再移除活动文件名，避免覆盖已有归档；要求文件系统支持硬链接。发布失败会记录 ERROR 和 I/O 错误指标，保留原文件并继续处理后续日志。发布后清理活动文件名失败时，`.log` 已经是可安全转移的完整文件，可能同时留下指向相同数据的 `.active` 名称。

写入失败时，只清理本 Writer 创建、从未报告写入字节、确认大小为零、成功关闭且文件身份未变化的分段，避免空故障文件持续积累。历史文件、非空分段和无法确认安全的分段均保留。进程崩溃或写入、同步、关闭失败遗留的 `.active` 不会自动续写或发布，应由运维在确认没有活动 Writer 后检查、隔离或恢复，并为这类文件制定容量告警和回收策略；不要批量去掉后缀。库不会修改历史文件。

同文件系统可以直接重命名搬走已封存的 `.log`；跨文件系统或远端上传时，转移端应先完成复制、校验和目标端持久化，再删除源文件。

## 写入结构与资源上限

```text
并发生产者 → 有界字节环形队列 + 有界记录元数据
           → 单个后台协程 → 合并为连续批次 → .log.active → 同步/关闭 → .log
```

| 配置 | 默认值 | 作用 |
|---|---:|---|
| `QueueBytes` | 32 MiB | 队列中有效载荷内存上限 |
| `QueueRecords` | 65536 | 记录元数据上限，限制小记录堆积 |
| `MaxRecordBytes` | 128 KiB | 单条规范化长度上限，含 LF |
| `BatchBytes` | 1 MiB | 写盘批次缓冲上限 |
| `BatchRecords` | 4096 | 每批记录条数上限 |
| `FlushInterval` | 100 ms | 磁盘跟得上时，低流量的最大合批等待时间 |
| `RecoveryInterval` | 100 ms | I/O 失败后的冷却时间；冷却期内待写批次丢弃，之后下一批尝试新分段 |
| `ErrorLogger` | 独立 stderr logger | 可传入 `*slog.Logger` 接收 ERROR 诊断；不能回写到同一个 qlog Writer |
| `SyncEveryBatch` | false | 是否每批 `fsync` 后才计成功 |

初始化预分配主要数据结构：约 32 MiB 队列 + 1 MiB 批次 + 1056 KiB 元数据，合计约 34 MiB（64 位环境，含每条记录的入队分钟及批次长度数组）。这是库的主要缓冲大小，不是整个进程 RSS 承诺；Go runtime、Prometheus 抓取、调用方及内核页缓存另计。数据不按每条单独分配；错误诊断只保留一份错误样本和累计次数，内存不会随积压持续增加。每个 Writer 一个写入 goroutine 和一个独立诊断 goroutine；写入使用合批和分钟轮转定时器，诊断使用限频定时器，通常仅打开一个日志文件。诊断只保留错误样本和计数，不持有 Writer 的队列、批次缓冲。

配置的队列、批次和记录元数据预分配预算不能超过 1 GiB，否则 `New` 返回错误。预算按 `QueueBytes + BatchBytes + 16 × QueueRecords + 8 × BatchRecords` 计算，在 32 位环境中也采用这个保守口径。该限制用于拒绝过大的配置，不检测剩余内存；多个 Writer、Go runtime 和业务内存仍需由调用方统一规划。

后台先取得队列快照，在锁外扫描记录边界并复制数据，复制完成后再释放队列容量；期间生产者可以追加到未占用区域。`QueueRecords` / `QueueBytes` 包含正在复制但尚未释放的队列记录，`Pending*` 包含全部尚未完成的记录。

默认配置以常规业务日志的吞吐和内存平衡为目标。128 MiB/分钟约为 2.13 MiB/s，100 ms 内约积累 218 KiB，低于 1 MiB 批次上限；无需等批次满才写盘。队列从空开始且未先触及条数上限时，32 MiB 队列可吸收约 15 秒该流量下的写盘阻塞，或约 3 秒的五倍峰值；平均每条小于 512 字节时，65536 条记录上限会先成为限制。这些缓冲只能吸收写盘变慢或阻塞，显式 I/O 错误和冷却期仍按失败处理，不保留记录等待恢复。如果持续输入高于输出，队列满后丢弃新日志；内存不会无限增加，生产者也不会因等待空位而堆积。业务自身持有的数据以及多个 Writer 的缓冲仍需计入进程内存预算。

## 成功、失败和退出语义

- `Write` 返回成功：库已处理该记录，可能入队，也可能因队列满而丢弃；不是落盘或不丢失的保证。队列满的记录同时计入 `Accepted*` 与 `Failed*`。
- 默认 `written_*`：完整记录被 `os.File.Write` 接收，通常进入内核页缓存。轮转和正常退出都会 `fsync`。`sealed_*` 只在分段 `fsync` + `Close` + 发布 `.log` 全部成功后累加，表示已可安全转移的数据。发布后删除 `.active` 名称失败计为清理错误，已发布分段仍计入 `sealed_*`。当前健康分段在封存前也会产生 `written - sealed` 差额。
- `SyncEveryBatch=true`：当前批次 `fsync` 成功后才增加 `written_*`；代价是吞吐与尾延迟取决于 SSD 的实际同步性能。
- 打开、写入和同步操作各尝试一次。任何 I/O 错误或短写都会记录 ERROR 并放弃当前操作；库不重试当前批次，也不区分瞬态错误与永久错误。因此短暂故障也可能导致当前记录丢失。
- 操作失败后，放弃受影响的批次或分段并继续运行。关闭受损分段时不再尝试修复或封存，符合安全条件的空分段被清理，其他 `.active` 留待检查；后续记录使用新分段。同步阶段的 `ENOSPC` / `EDQUOT` 可能表示此前写入的延迟失败，不能靠再一次 `fsync` 成功来证明文件完整。`Close` 始终只调用一次。
- 每次操作失败后冷却 `RecoveryInterval`，冷却期间 worker 持续取出并丢弃队列中的**所有**批次（不只是故障时的当前批次），直到冷却结束；冷却期间到达的新记录通常会被丢弃，但批次未满且尚未到达 `FlushInterval`／批次上限／分钟边界时，记录可能滞留在批次中，并在冷却结束后或 `Shutdown` 排空时写入新分段。增大 `RecoveryInterval` 仅能减少文件打开尝试频率，不能保留冷却期间的记录以待恢复。冷却结束后由下一批日志触发恢复尝试；无需重新创建 Writer。后续批次的恢复不会补写已失败的记录。
- 每个 Writer 的 ERROR 日志最多每秒输出一次，后台处理首个错误时输出，关闭后尽力输出尚未输出的汇总；包含目录、日志流、操作、错误样本、事件数和失败计数。I/O 错误与丢弃批次/记录都纳入诊断事件。默认使用独立的 stderr logger，不依赖全局 `slog.Default()`，避免业务把全局 logger 指向 qlog 后形成递归。`ErrorLogger` 可接业务现有的独立日志通道。输出由独立诊断协程完成且不持有队列锁；即使诊断输出阻塞，写入协程仍可恢复写盘，`Shutdown` 也不等待诊断输出。自定义 handler 若永久阻塞，会占用该 Writer 的一个诊断协程直到返回；不会为后续错误创建更多协程或积累错误消息。进程立即退出时，尚未完成的诊断可能丢失。
- `Err()` / `Stats.Failed` / `qlog_failed` 仅反映当前未恢复的 I/O 故障，不表示 Writer 已终止。打开、写入、同步、关闭、封存分别记录故障，只在对应操作成功后清除；清理操作（空文件 Stat、删除空文件、删除已发布分段的 `.active` 名称）失败是一次性事件，只计入 `IOErrors` 与 ERROR 诊断，不进入未恢复故障状态。写入成功不会清除同步或封存故障；`Err()` 返回最早尚未恢复的错误。历史 `IOErrors`、`FailedRecords` 和 `FailedBytes` 永久累计。`WriteCalls` / `SyncCalls` 统计实际调用次数，耗时不包含故障冷却。
- 短写/部分写不续写剩余字节，也不重放已写前缀。默认模式只把完整前缀记录计为成功，剩余整条计为失败；`failed_bytes_total` 为失败记录的完整逻辑大小。严格同步模式下，写入或同步失败的整个批次计为失败。`partial_write_bytes_total` 累计异常写调用报告的字节数，非法计数限制在本次请求长度范围内，但不作为记录成功的依据。
- 故障记录不会自动补回，故障 `.active` 不会自动续写或发布，失败计数也不代表磁盘上绝对没有相关字节。默认模式在轮转或退出时同步/关闭失败，不倒扣之前的 write 成功计数，应结合 ERROR 日志和 `IOErrors` 检查。
- `Shutdown` 是停止 Writer 的正常入口，可重复、可并发调用；传入 nil context 返回 `ErrNilContext` 且不会停止 Writer。I/O 故障不通过其返回值传播；只有调用方 context 超时/取消会返回错误。超时只停止当前调用等待，后台继续处理，可再次调用等待完成。操作系统阻塞中的 write/fsync 无法被 context 安全中断。诊断输出独立收尾，库不会等待 handler 返回。
- 配置不合法、单条记录超长、显式关闭后调用仍返回错误。`Shutdown` 保留 context，用于限制关闭等待时间。
- 异步内存队列不提供进程崩溃/SIGKILL 前未落盘数据的保证。新文件及目录项的突然断电持久性还涉及父目录 fsync，本库不提供跨断电事务承诺。

`Stats.Closing` 表示已停止接收新记录，`Stats.Closed` 表示写入协程已完成排空和文件关闭处理；关闭排空期间可能同时出现 `Closing=true`、`Closed=false` 和非零 `PendingRecords`。关闭时的 I/O 失败仍须检查 `Failed` 和 `IOErrors`，`Closed` 不保证所有文件封存成功。

需要可靠投递的审计/交易数据不应依赖此尽力写入模式。使用失败计数和 ERROR 诊断监控丢失情况。

计数不变量（每个 Writer 的 lifetime totals）：

```text
received = accepted + rejected
accepted = written + failed + pending
```

records 与 bytes 均满足上述关系。`received` 在每次调用完成入队、丢弃或拒绝处理时计数。所有 counters 都是当前 Writer 的日志流级累计值，不扫描或累计以前进程留下的数据。

`sealed <= written` 是另一个独立维度：只有成功发布为 `.log` 的分段才计入 `sealed`。全程健康且完成 `Shutdown` 后两者相等。运行期间，`written - sealed` 包括当前尚未封存的健康分段，以及放弃分段中已计为写成功的完整记录。短写的记录片段、严格同步模式中计为失败的批次也可能有字节留在磁盘，实际残留大小需检查文件确认。

## Prometheus 与 Grafana

```go
registry := prometheus.NewRegistry()
registry.MustRegister(
    qlog.NewCollector(w),
    collectors.NewGoCollector(),
    collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
)
mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
```

库不会注册全局 registry、开 HTTP 端口或修改业务服务。可运行 `cmd/loadgen` 参考完整接入，包括 SIGINT/SIGTERM、HTTP 退出和错误处理。

所有累计 counters 统一携带 `directory`（例如 `/data/request`）和 `stream`（例如 `request`）标签：

| 指标名称 | 含义 |
|---|---|
| `qlog_stream_received_records_total`, `qlog_stream_received_bytes_total` | 输入日志条数/大小 |
| `qlog_stream_accepted_records_total`, `qlog_stream_accepted_bytes_total` | 已处理条数/大小，含队列满丢弃 |
| `qlog_stream_written_records_total`, `qlog_stream_written_bytes_total` | 写成功条数/大小 |
| `qlog_stream_sealed_records_total`, `qlog_stream_sealed_bytes_total` | 已发布为 `.log`、可安全转移的条数/大小 |
| `qlog_stream_failed_records_total`, `qlog_stream_failed_bytes_total` | 队列满、冷却期丢弃及后台写失败的条数/大小 |
| `qlog_stream_rejected_records_total`, `qlog_stream_rejected_bytes_total` | 已关闭或非法输入等返回错误的条数/大小 |

查看日志流的每秒写入速率：

```promql
rate(qlog_stream_received_records_total{stream="request"}[1m])
rate(qlog_stream_received_bytes_total{stream="request"}[1m])
rate(qlog_stream_written_records_total{stream="request"}[1m])
rate(qlog_stream_written_bytes_total{stream="request"}[1m])
```

`rate` 是窗口内的平均每秒速率，需要至少两次抓取，并非精确到每一秒的事件桶；建议 5s 抓取间隔，Grafana 使用 `$__rate_interval`。

其他指标包含队列条数/大小、未完成条数/大小、I/O 错误、写盘/同步调用数及总耗时。CPU 使用 `rate(process_cpu_seconds_total[1m])`（占用的 CPU 核数）；内存使用 `process_resident_memory_bytes` / `go_memstats_heap_alloc_bytes`。进程指标属于整个应用，不是单独某个 Writer。

直接导入 [`monitoring/grafana-dashboard.json`](monitoring/grafana-dashboard.json)，选择 Prometheus 数据源、directory 和 stream；包含流级速率、丢弃速率与丢弃率、队列使用率、已封存速率、进程内存/CPU、队列、I/O 错误以及未恢复故障状态。Prometheus 示例配置 [`monitoring/prometheus.yml`](monitoring/prometheus.yml) 加载同目录的 [`qlog-alerts.yml`](monitoring/qlog-alerts.yml)：`qlog_failed == 1` 持续 1 分钟后触发 `QlogUnresolvedIOFailure`，恢复为 0 后解除，空闲时仍保留故障告警；`qlog_stream_failed_records_total` 在 5 分钟内有增长并持续 2 分钟触发 `QlogDroppingRecords`（覆盖队列满、冷却期丢弃和写失败）；`qlog_queue_bytes / qlog_queue_capacity_bytes > 0.9` 持续 5 分钟触发 `QlogQueueNearCapacity`。告警通知路由由使用方的 Alertmanager 配置。

多个实例应使用不同目录，不能共同写同一目录。

## 性能容量与验收

100 MB/s SSD、8192 字节一条（含 LF），理论上限约为 **12,207 条/秒**；若设备参数是 100 MiB/s，则为 12,800 条/秒。按 80% 带宽预算，建议从 **约 9,765 条/秒** 开始验收。磁盘其他读写、文件系统、fsync、容器限制都会影响结果。

```bash
make test        # 功能、故障注入、并发 race、覆盖率
make vet
make lint       # 需要 golangci-lint v2
make fuzz
make check-monitoring # 需要 promtool；检查告警配置、持续故障及恢复行为
make bench      # 内存队列基准，无真实磁盘，不能当作 SSD 吞吐
make bench-batches # 对比 1/4/16/64 MiB 批次的内存队列开销
make build

# 指向目标 SSD；默认每条 8192 字节，10,000 条/s。
# 默认 10 秒约生成 800 MB 数据；60 秒约 4.9 GB。
./bin/loadgen -base /data -name request -duration 60s -rps 10000
./bin/loadgen -base /data -name durable -duration 60s -rps 10000 -sync
# 不限速提交，观察实际 written 吞吐与 failed 丢弃量；包含最终 fsync 和 Shutdown。
./bin/loadgen -base /data -name capacity -duration 60s -rps 0
```

当前基准按每条 8 KiB 测量，在计时前等待后台初始化和首批写入完成，计时包含最终 Shutdown。`ns/op` 为提交开销，可能包含队列满时快速丢弃的调用；同时报告 `written-MB/s` 和 `dropped-%`，不能把提交速率当作实际写入吞吐。`QueueBytes` 用于吸收磁盘停顿，`BatchBytes` 决定合批大小，可以独立调整；不必一起增大。可从默认的 1 MiB 批次开始，与 4/16/64 MiB 对比，观察业务写入延迟、CPU 和真实磁盘吞吐。较小批次可减少缓冲内存和单次 I/O 的数据量，但会增加写调用频率；`SyncEveryBatch=true` 时也可能增加 fsync 次数，必须在目标盘复测。

`loadgen` 完成运行后先输出统计 JSON；若实际写入为零、发生丢弃、有未完成数据、出现 I/O 错误、`Sealed*` 与 `Written*` 不一致或计数不一致，则以非零退出码报告验收失败。不限速提交也执行该判定，出现丢弃时可从 JSON 查看实际吞吐。库本身的尽力写入行为不变。

验收时同时检查：退出成功、`WrittenRecords == AcceptedRecords`、`FailedRecords == 0`、`PendingRecords == 0`、`SealedRecords == WrittenRecords`、`SealedBytes == WrittenBytes`、最终封存文件字节数与 `SealedBytes` 相等；稳态无队列持续上升，CPU/RSS 达到业务预算。故障场景下应结合 `WrittenBytes - SealedBytes`、失败计数、ERROR 诊断及文件检查评估残留数据。压测在到时或信号取消后停止提交，再等待后台处理完队列。30 秒 shutdown deadline 足以覆盖正常排空，长时间磁盘挂起会报错。

验收范围、复现步骤与判定标准见 [`VALIDATION.md`](VALIDATION.md)。日志保留天数、压缩、远端备份及非空故障文件的回收由外部策略负责；库仅自动清理上述符合条件的空失败分段，不删除已有日志数据。
