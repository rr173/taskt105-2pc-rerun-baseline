# 2PC Coordinator

可恢复的两阶段提交协调器：注册参与者、创建事务、执行 prepare/commit/abort，并在重启后恢复未完成事务。状态和参与者记录持久化在 SQLite 中。

## 本地构建、测试与运行

```bash
go test ./...
go vet ./...
go build ./...
go run . --smoke-test
go run . --db ./2pc.db --addr :8080
```

`--smoke-test` 会通过 HTTP 层执行事务提交、回滚、恢复和幂等性检查，完成后自动退出；常规运行启动 HTTP 服务。

## Benzhi Docker

```bash
bash ./build_benzhi_docker.sh go-task105-2pc:amd64 linux/amd64
docker run --rm go-task105-2pc:amd64
```

`build_benzhi_docker.sh` 使用 `benzhi.Dockerfile` 构建镜像；容器启动后进入 shell，可在容器内执行上述构建、测试和自检命令。
