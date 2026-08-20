# 官方 Go 镜像，自带完整工具链
FROM golang:1.26.3

WORKDIR /app

# 评测构建允许自动下载 Go 依赖；指定国内代理与校验和数据库以保证容器内可用
ENV GOPROXY=https://goproxy.cn,direct
ENV GOSUMDB=sum.golang.google.cn
ENV GOTOOLCHAIN=local
ENV CGO_ENABLED=0

# 先复制依赖文件并下载依赖，利用 Docker 缓存并保证容器内可用
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# 预编译一次，把编译缓存留在镜像里；不影响模型修改源码
RUN go build ./...

# 容器启动后进入 shell，方便操作
CMD ["bash"]
