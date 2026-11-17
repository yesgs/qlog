# 当前版本验收

本文描述当前实现的验收范围、复现步骤和判定标准。运行环境、代码版本与结果应在执行验收时一并记录；本文不提供未经当前版本复测的性能数字。

## 验收范围

| 范围 | 当前行为与检查重点 |
|---|---|
| 输入与配置 | 输入复制、LF/CRLF 规范化、多行记录、长度限制、跨平台名称校验、1 GiB 预分配预算 |
| 队列与并发 | 队列满时丢弃、锁外复制、输入复用、环形回绕、并发生产与关闭、记录及字节计数不变量 |
| 文件生命周期 | 按入队分钟轮转、空闲封存、时区与时钟回拨、重启创建新分段、已有文件不被覆盖或追加 |
| I/O 故障与恢复 | 打开、写入、同步各尝试一次；短写和 I/O 故障计失败；冷却后由后续批次尝试新分段 |
| 封存与清理 | 写入、同步和关闭成功后发布 `.log`；故障分段保留 `.active`，仅安全清理符合条件的空文件 |
| 退出 | 停止接收、排空与关闭状态区分；等待超时后后台继续处理；诊断输出阻塞不阻塞写入和退出 |
| 可观测性 | Prometheus 指标（含 `sealed_*`）、Grafana 指标引用、持续 I/O 故障告警及恢复、丢弃与队列水位告警 |
| CLI | 健康写入通过验收；零写入、丢弃、未完成记录或 I/O 故障以非零退出码报告 |

记录数与字节数均应满足：

```text
received = accepted + rejected
accepted = written + failed + pending
```

`sealed <= written` 独立校验。成功发布为 `.log` 的分段计入 `sealed`，包括发布后删除 `.active` 名称失败的分段，清理错误计入 `IOErrors` 与 ERROR 诊断。全程健康且完成 `Shutdown` 后 `sealed == written`。运行期间差值包括当前尚未封存的健康分段，以及放弃分段中已计为写成功的完整记录。短写的记录片段、严格同步模式中计为失败的批次也可能有字节留在磁盘，实际残留大小需检查文件确认。

## 工程检查

需要 Go 1.26.11+；lint 需要 golangci-lint v2，监控配置检查需要 promtool。

```bash
make test             # race 与语句覆盖率
make vet
make lint
make fuzz             # 队列和名称校验
make check-monitoring # Prometheus 配置、持续故障和恢复
make build

go mod verify
```

CI 在 Linux、macOS、Windows 上执行构建、vet、模块校验及 race 测试，并分别执行 lint、govulncheck、fuzz 和监控配置检查。覆盖率以实际输出为准，库包与 `cmd/loadgen` 分别统计。

## 真实文件验收

在 Linux 上运行：

```bash
make build
python3 scripts/validate.py
```

脚本分别以普通批量模式和逐批同步模式运行 5 秒，每条 8192 字节、每秒 10,000 条，每种模式约生成 410 MB 临时日志。检查内容包括：

- 真实 HTTP `/metrics` 中的日志流 counters、进程 CPU、RSS 和 Go heap 指标。
- 封存文件内容、记录条数与字节数和 Writer 统计一致。
- `AcceptedRecords == WrittenRecords`、`SealedRecords == WrittenRecords`、`SealedBytes == WrittenBytes`、`FailedRecords == 0`、`PendingRecords == 0`。
- 实际 SIGTERM 后正常排空、封存并退出。

脚本仅清理自己创建的临时日志目录；统计 JSON、指标快照和诊断输出保存在本地 `validation/`，该目录不纳入版本控制。

## 性能与目标盘验收

```bash
make bench
make bench-batches
```

基准使用模拟文件，衡量队列、内存复制、计数和协程开销。提交速率可能包含队列满时被丢弃的调用，应同时查看 `written-MB/s` 和 `dropped-%`；模拟文件的吞吐不能用于判断真实磁盘能力。

目标 SSD 的持续吞吐需要在实际部署环境单独测量。按每条 8192 字节、100 MB/s 带宽估算，理论上限约为 12,207 条/秒；可从约 9,765 条/秒开始进行跨数分钟的验收，分别运行普通模式与逐批同步模式，步骤见 [README](README.md#性能容量与验收)。

健康运行应退出成功，无失败、无未完成记录，`SealedBytes == WrittenBytes` 且实际封存文件字节数与 `SealedBytes` 一致；持续观察队列积压、CPU、RSS 和其他磁盘负载，并将最终 fsync 与 Shutdown 耗时计入结果。短时本机测试不足以证明目标盘的持续写入能力。
