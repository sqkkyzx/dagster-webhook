# 隔离验收

这套验收会创建并终止测试 Run，仅使用脚本自己启动的 Dagster `1.13.26`。
脚本不接受业务服务 URL，清除继承的 Dagster/网关连接配置，使用临时存储与独立
测试 Job。原生模式使用随机本机端口；Docker 模式使用独立内部网络和命名卷，
不向宿主发布端口，HTTP 检查通过 `docker compose exec` 在代理容器内访问测试
服务。两种模式都会在结束时清理自己创建的资源。

## 成品镜像验收

需要 Docker Engine 和 Docker Compose v2。先构建应用镜像，再运行：

```bash
docker build -t dagster-webhook:acceptance .
python integration/check.py --mode docker --image dagster-webhook:acceptance
```

脚本启动指定的成品镜像，不重新构建应用。它会另外构建专用 Dagster/故障代理
测试镜像。CI 在 PR 和主分支运行同一验收，只有主分支且验收成功后才发布，发布
时直接给已经测过的本地镜像加标签并推送，避免验收后重建造成内容变化。

## 无 Docker 的本机验收

使用隔离 Python 环境安装项目依赖和 `dagster-webserver==1.13.26`：

```bash
uv venv /tmp/dagster-gateway-acceptance
uv pip install --python /tmp/dagster-gateway-acceptance/bin/python . dagster-webserver==1.13.26
/tmp/dagster-gateway-acceptance/bin/python integration/check.py --mode native
```

此模式启动真实 Dagster Webserver 和网关源码进程；可以验证 GraphQL 和执行
行为，但不代表成品镜像已通过验收。

## 检查内容

- 健康、readiness、ReDoc/OpenAPI 版本、位置和 Job 发现。
- 原生配置 Schema、正确/错误配置验证及结构化错误路径。
- 旧提交指定不存在的 Repository 时明确拒绝，不自动改投其他 Repository。
- 非幂等资源接口连续提交相同参数，创建两个不同 Run，且不注入提交恢复标记。
- 幂等 Key 可从 Body 提供，并使用 Header 重放到同一 Run；两处相同可重放，
  两处冲突或均未提供返回 422，且不向 Dagster 发送创建请求。
- 来源与提交关联保留标签在发往上游之前被拒绝。
- 同 Key 并发提交仅发送一次 launch mutation，运行列表和事件可读取。
- 查询每个 op 执行步骤的状态及所有输出事件，验证明确导出的 JSON、null、0 和 false
  值，以及没有导出的值明确标记为不可读取；小页游标遍历完整覆盖输出事件。
- 网关重启后使用持久化记录返回相同 Run。
- 代理在 Dagster 确认接受后丢弃 launch 响应，验证 UNKNOWN、submission 查询、
  重启、显式核对和同 Key 重放；代理统计确认没有第二次 launch。
- 创建独立等待任务，真实批量终止与不存在 Run 混合返回逐项结果。
- 停止独立 Dagster 后，readiness 返回 503，health 和本地提交记录查询继续可用。

`upstream_proxy.py` 的控制接口仅供测试，不会复制进应用镜像。
脚本失败会输出测试进程日志；所有进程/容器由本次脚本创建并负责清理。
