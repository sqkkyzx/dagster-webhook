# dagster-webhook

基于 FastAPI 的 Dagster HTTP API 网关，提供任务发现、配置校验、运行提交、运行查询、事件读取和运行控制。网关连接现有的 Dagster Webserver，任务定义、调度和执行仍由 Dagster 管理。

项目地址：<https://github.com/sqkkyzx/dagster-webhook>

## 启动与连接

要求 Python 3.13 或 3.14，当前依赖 Dagster `1.13.26`。

```bash
git clone https://github.com/sqkkyzx/dagster-webhook.git
cd dagster-webhook
uv venv
uv pip install .

export APP_API_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export DAGSTER_URL=http://127.0.0.1:3000
uv run --no-sync python main.py
```

保存生成的 `APP_API_TOKEN`，调用接口时使用同一个值。正式部署应通过环境变量或 Secret 管理固定 Token。未配置 `APP_API_TOKEN` 且未配置独立 API Key 时，服务会使用进程内临时 Token，并将临时值输出到启动日志；重启后 Token 会变化。仅配置独立 API Key 时，不启用临时全权限 Token。

可参考 `.env.example` 配置变量。程序本身不会自动加载 `.env`；可由部署平台注入，或使用 `uv run --no-sync --env-file .env python main.py` 显式加载。

上述安装方式按 `pyproject.toml` 安装依赖。当前 `uv.lock` 的平台范围为 Linux 和 Windows，Dockerfile 沿用 `uv pip install .`，不保证严格安装锁文件中的全部版本。

### HTTPS、反向代理与上游鉴权

```bash
export DAGSTER_URL=https://dagster.example.test/dagster
export DAGSTER_HEADERS_JSON='{"Authorization":"Bearer <upstream-token>"}'
# 使用私有 CA 时再设置：
# export DAGSTER_CA_BUNDLE=/path/to/ca-bundle.pem
```

`DAGSTER_URL` 表示 Dagster 部署根地址，可包含路径前缀；尾部的一个 `/graphql` 会自动规范化，避免重复添加。配置 URL 时优先使用 URL 中的主机、端口、协议和路径；未配置 URL 时兼容 `DAGSTER_HOST` 和 `DAGSTER_PORT`。URL 不接受用户名、密码、查询参数或 fragment，上游认证信息通过请求头传递。

TLS 证书校验默认开启。未指定 CA bundle 时使用系统／请求库信任设置，不提供跳过证书校验的配置。网关的入站 API Token 和访问 Dagster 的上游凭据分别配置。

## Docker

```bash
docker build -t dagster-webhook:local .
docker volume create dagster-webhook-data

docker run --rm --name dagster-webhook \
  --network dagster-network \
  -p 8080:8080 \
  -e APP_API_TOKEN \
  -e DAGSTER_URL=http://webserver:3000 \
  -v dagster-webhook-data:/data \
  dagster-webhook:local
```

将 `dagster-network` 替换为 Dagster Webserver 所在的 Docker 网络。镜像以 `python main.py` 启动，默认监听 8080。容器中的幂等数据库放在 `/data`，必须保留持久卷，才能在容器重建后继续识别已处理的请求。

## 环境变量

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `APP_API_TOKEN` | 未配置独立 Key 时生成临时值 | 兼容的全权限 Bearer Token，生产部署应设置固定值或独立 Key |
| `APP_API_KEYS_JSON` | 未配置 | 独立调用方的 Token、名称和权限范围，见下文 |
| `APP_HOST` | `0.0.0.0` | 网关监听地址 |
| `APP_PORT` | `8080` | 网关监听端口 |
| `DAGSTER_URL` | 未配置 | 完整的 HTTP(S) 部署 URL，优先于 host／port |
| `DAGSTER_HOST` | `webserver` | 兼容的上游主机名或 IP，不含协议及路径 |
| `DAGSTER_PORT` | `3000` | 未配置 URL 时使用的上游端口 |
| `DAGSTER_REPOSITORY_NAME` | `__repository__` | 旧提交接口省略或传入 `null` 的 `repository_name` 使用此值；显式请求值优先 |
| `DAGSTER_TIMEOUT` | `120` | 上游请求超时，单位为秒，必须大于 0 |
| `DAGSTER_READINESS_TIMEOUT` | `3` | readiness 上游检查超时，单位为秒 |
| `DAGSTER_HEADERS_JSON` | `{}` | 上游 HTTP 请求头的字符串字典，可配置鉴权头 |
| `DAGSTER_CA_BUNDLE` | 未配置 | 可选 CA bundle 文件路径，文件必须存在 |
| `IDEMPOTENCY_DB_PATH` | `./data/idempotency.sqlite3` | 幂等记录的 SQLite 文件路径；容器默认位于 `/data` |
| `IDEMPOTENCY_RETENTION_SECONDS` | `86400` | 已完成幂等记录的保留期，单位为秒 |
| `IDEMPOTENCY_LEASE_SECONDS` | `30` | 处理请求的租约时间，过期后允许核对已有运行 |

## API 文档与权限

- ReDoc：<http://localhost:8080/redoc>
- Swagger UI：<http://localhost:8080/docs>
- OpenAPI JSON：<http://localhost:8080/openapi.json>

ReDoc 包含字段含义、参数约束、请求／响应示例、错误码、分页、幂等和权限语义。每次接口实现和契约调整都必须同步更新 ReDoc 的参数、示例、响应和权限说明，并通过文档契约测试。Swagger UI 的 **Authorize** 输入网关 Token 即可调试业务接口。

所有 `/api/v1/` 接口要求请求头：

```http
Authorization: Bearer <gateway-token>
```

可配置独立调用方：

```bash
export APP_API_KEYS_JSON='[
  {"name":"report-reader","token":"<read-token>","scopes":["read"]},
  {"name":"job-trigger","token":"<submit-token>","scopes":["read","submit"]}
]'
```

`read` 用于发现、配置校验和运行读取；`submit` 用于提交运行；`cancel` 用于终止运行；`admin` 包含全部权限，并允许代码位置管理。兼容的 `APP_API_TOKEN` 具有全部权限。调用方 `name` 是幂等隔离的稳定身份；轮换 Token 时保留名称，避免丢失该调用方的幂等关联。不同配置项不能使用重复名称，`legacy` 为兼容 Token 保留。

提交状态查询与显式核对使用 `submit` 权限，方便提交方处理自己的请求；它们按调用方 `name` 隔离，`admin` 也不能查询其他身份的提交记录。Key 配置在启动时读取，删除或替换 Token 后需重启网关，当前不提供新旧 Token 同身份并存的轮换窗口。

## 资源接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 网关进程存活与版本，无需上游请求 |
| `GET` | `/ready` | 短超时检查 Dagster 是否可服务 |
| `GET` | `/api/v1/locations` | 列出代码位置及加载状态 |
| `GET` | `/api/v1/jobs` | 发现 Job，可按位置／Repository 筛选 |
| `GET` | `/api/v1/jobs/{job_id}` | 查询 Job 详情 |
| `GET` | `/api/v1/jobs/{job_id}/config-schema` | 查询 Job 的配置类型和字段 |
| `POST` | `/api/v1/jobs/{job_id}/validate` | 提交前校验配置和执行选择，不创建 Run |
| `POST` | `/api/v1/runs` | 带幂等控制地创建 Run |
| `GET` | `/api/v1/runs` | 过滤、分页查询运行列表 |
| `GET` | `/api/v1/runs/{run_id}` | 查询运行详情、时间和失败摘要 |
| `GET` | `/api/v1/runs/{run_id}/events` | 按游标增量读取事件 |
| `GET` | `/api/v1/submissions/{submission_id}` | 只读本地提交记录，无需原请求配置 |
| `POST` | `/api/v1/submissions/{submission_id}/reconcile` | 查询已有提交的运行，更新核对结果，不创建任务 |
| `POST` | `/api/v1/runs/terminate-batch` | 批量终止，逐项返回结果 |

`/health` 只检查进程。上游不可达时，进程仍可存活，而 `/ready` 返回不可服务；两者用途不同。每个业务请求使用独立的 Dagster 客户端，避免并发请求共享 gql 连接状态。

### 发现与校验

```bash
curl --fail-with-body http://localhost:8080/api/v1/jobs \
  -H "Authorization: Bearer $APP_API_TOKEN"
```

Job 列表返回的 `job_id` 是“代码位置＋Repository＋Job 名称”的 URL 安全编码，用返回值调用详情和提交接口。它不是 Job 名称；同名 Job 位于不同代码位置时具有不同 ID。

```bash
curl --fail-with-body "http://localhost:8080/api/v1/jobs/$JOB_ID/config-schema" \
  -H "Authorization: Bearer $APP_API_TOKEN"

curl --fail-with-body -X POST "http://localhost:8080/api/v1/jobs/$JOB_ID/validate" \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"run_config":{}}'
```

配置 Schema 返回的是 Dagster 原生类型图及字段信息，不宣称它是标准 JSON Schema。外层请求拒绝未知字段，例如误写为 `run_confg` 会返回 `422`；`run_config` 内部的合法字段由目标 Job 的配置规则决定。校验接口完成检查后返回 `200`，通过 `valid` 和 `errors[{message,path,reason}]` 表达配置是否有效；`valid=false` 不会创建 Run。校验通过不保证后续执行一定成功，执行时还可能遇到数据、资源或代码变化。

配置错误的 `path` 从 Dagster 的验证栈转换：对象字段和 Map 键为字符串，数组下标为整数，例如 `["ops","extract","config","items",0,"count"]`。缺失或未知字段附加到所属对象路径，多字段错误逐项返回。根级错误的路径可以为空。提交时的配置错误通过 `details[].path` 使用同一规则。

### 幂等提交

```bash
curl --fail-with-body -X POST http://localhost:8080/api/v1/runs \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  -H 'Idempotency-Key: source-event-20261108-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id":"<copy-from-jobs-response>",
    "run_config":{},
    "tags":{"source":"webhook"}
  }'
```

首次创建成功返回 `201`，重复请求命中已有结果返回 `200`。响应包含 Run 信息和重放标记；重放返回已记录的提交结果快照，不保证状态为最新值。Run 创建成功只表示已提交执行，最终成功或失败通过运行详情查询确认。

新旧提交接口均禁止传入保留来源标签 `.dagster/repository` 和提交关联标签 `dagster-webhook/submission_id`，否则返回 `422`。来源标签由 Dagster 生成，提交关联标签由幂等接口注入；其他普通标签仍可使用。

幂等键为必填请求头，长度为 1–200 个字符，仅允许字母、数字、点、下划线、冒号和短横线。键在同一调用方内生效：同键、同参数返回已有 Run；同键、不同参数返回 `409 IDEMPOTENCY_PAYLOAD_MISMATCH`。每次提交前先通过 SQLite 原子占位并持久化 `submission_id`，只允许占位成功的请求发送一次创建操作；远程请求期间不持有数据库事务锁。同键正在处理时返回 `409 IDEMPOTENCY_IN_PROGRESS` 和 `Retry-After`，等待后使用同键重试。

Dagster 1.13.26 的 GraphQL 提交不支持客户端指定 Run ID。新接口将 `submission_id` 写入受保护标签 `dagster-webhook/submission_id`，并在成功响应后保存 Dagster 分配的真实 Run ID。标签只用于恢复关联，防止并发重复提交依赖本地原子占位。

如果首次提交超时，或 Dagster 已响应但后续存储写入失败，接口返回 `409 SUBMISSION_OUTCOME_UNKNOWN`。进程中断或租约到期也按结果不确定处理。后续使用相同键重试时只按提交关联标签查找已有运行，核对唯一匹配、Job 来源和状态，不自动再次创建。仅找到 `NOT_STARTED`、查无运行、匹配不唯一或来源不一致，都不能确认提交；无法核实时也不会重提。

不确定响应的 `details` 提供 `submission_id`，尚未确认的 `run_id` 为 `null`。调用方应保留提交 ID 继续核对，不能把查询无结果当作“未执行”而更换键提交。不要在 Dagster 中手工删除或修改网关的提交关联标签，否则自动核对可能无法完成。

若持久化存储在创建提交占位记录之前就失败，则返回 `503 PERSISTENCE_UNAVAILABLE`，不会联系 Dagster 创建 Run。已完成占位或可能联系上游之后发生的失败，按结果不确定处理，不能简单当作可重新创建的存储故障。

保留期到期后，已接受或明确拒绝的记录可被清理，原键可能用于新提交；调用方应为不同业务事件使用不同键。不确定记录不会仅因保留期到期而自动删除。幂等记录不保存原始任务配置，只保存请求摘要和恢复所需信息。

当前存储适用于单机持久化部署，以及同机进程共享同一 SQLite 文件。不同主机上的副本不应各用独立数据库后宣称具有跨副本幂等保证，也不支持通过网络文件系统共享 SQLite 文件。跨主机持久化后端属于后续扩展。

### 提交状态查询与核对

保存创建响应中的 `submission_id`、`submission_url`；结果不确定或明确拒绝的响应在 `details` 中提供提交 ID。真实 Run ID 未确认时为 `null`。查询提交记录不需要重新发送任务配置或幂等键：

```bash
curl --fail-with-body "http://localhost:8080/api/v1/submissions/$SUBMISSION_ID" \
  -H "Authorization: Bearer $APP_API_TOKEN"

curl --fail-with-body -X POST \
  "http://localhost:8080/api/v1/submissions/$SUBMISSION_ID/reconcile" \
  -H "Authorization: Bearer $APP_API_TOKEN"
```

`GET` 只读取本地记录，不联系 Dagster、不续期。响应包含 `state`、`run_id`、创建时间、保留截止时间、最近核对时间和 `last_check_outcome`；`run` 是只含安全字段的状态快照，不包含任务配置或标签。其他调用方、记录不存在、已完成记录过期均返回相同的 `404`。

| state | 含义与后续操作 |
| --- | --- |
| `REQUESTING` | 仍在有效租约内处理；核对返回 `409` 和 `Retry-After` |
| `ACCEPTED` | 已确认 Dagster 接受提交；实际运行状态通过 `status_url` 查询，需要 `read` 权限 |
| `REJECTED` | Dagster 明确拒绝；提供错误类型，修正业务请求后使用新 Key |
| `UNKNOWN` | 仍无法确认；可继续显式核对，不自动过期或重新提交 |

`POST /reconcile` 无需请求体，只查询该记录关联的运行。已接受／明确拒绝记录直接返回快照，不重复查询或续期。查无运行、仅找到 `NOT_STARTED`、查询异常时，状态仍为 `UNKNOWN`，可查看 `last_check_outcome`；这些情况均不能证明任务未执行。正常核对返回 `200`，应检查 `state`，不能据此认定业务执行成功。

核对后的持久化失败保持 `409 SUBMISSION_OUTCOME_UNKNOWN` 和提交 ID；读取本地存储失败可返回 `503`。核对不会创建或终止 Run，也不会释放原幂等键。没有自动重提或人工释放键接口。升级时自动兼容现有 SQLite 表；旧记录仍按原恢复标识处理，不会自动重发。旧快照缺少的时间字段可为空。

### 运行列表、详情和事件

```bash
curl --fail-with-body --get http://localhost:8080/api/v1/runs \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  --data-urlencode "job_id=$JOB_ID" \
  --data-urlencode 'statuses=FAILURE' \
  --data-urlencode 'tag=source=webhook' \
  --data-urlencode 'limit=20'

curl --fail-with-body "http://localhost:8080/api/v1/runs/$RUN_ID" \
  -H "Authorization: Bearer $APP_API_TOKEN"

curl --fail-with-body --get "http://localhost:8080/api/v1/runs/$RUN_ID/events" \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  --data-urlencode 'limit=100'
```

运行列表的 `limit` 默认为 50，范围 1–200；事件列表默认为 100，范围 1–1000。两类列表返回 `items`、`next_cursor`、`has_more`。运行列表在还有下一页时返回最后一个 Run ID 作为 `next_cursor`，没有下一页时为 `null`。事件列表返回 Dagster 不透明游标，即使当前 `has_more=false` 也可能保留 `next_cursor`，供后续继续轮询新事件。把游标原样传给下一次请求，两类游标不能互换。运行状态可能在分页过程中变化，游标分页不代表冻结的数据快照。代码位置和 Job 发现接口返回 `items`，不伪造分页能力。

可重复传入 `statuses` 选择多个状态，例如 `?statuses=SUCCESS&statuses=FAILURE`；可重复传入 `tag=key=value` 组合标签条件。推荐用发现接口返回的 `job_id` 精确筛选“位置＋Repository＋Job”；`job_name` 过滤会匹配多个位置中的同名 Job，不代表唯一 Job 身份。`job_id` 与同时提供的 `job_name` 或保留 Repository 标签存在冲突时返回 `422`。

精确 Job 筛选采用 Dagster 原生运行列表的 Repository 来源标签。缺少该来源标签的旧运行不会匹配 `job_id`，可改用 `job_name` 查询，再根据详情判断来源。

详情提供运行状态、开始／结束时间、标签、父／根 Run ID、失败摘要和来源字段 `job_id`、`repository_location_name`、`repository_name`；历史运行缺少来源时，这些来源字段可为 `null`。列表／详情不会默认返回整份运行配置。事件接口提供 Dagster 结构化事件，原始 stdout／stderr 的完整下载不在该接口契约中。运行 `SUCCESS` 也不表示网关能够直接返回任意 Python 业务输出，输出读取需依据资产元数据或业务约定。

### 批量终止

```bash
curl --fail-with-body -X POST http://localhost:8080/api/v1/runs/terminate-batch \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"run_ids":["run-id-1","run-id-2"]}'
```

新接口一次接受 1–200 个不同的 Run ID，重复 ID 返回 `422`。逐项返回 `results`，每项包含 Run ID、`accepted`／`failed` 状态及错误信息，并返回成功／失败计数。批次完成处理时返回 `200`，即使其中部分运行终止失败；请求自身参数、鉴权或上游传输失败仍使用相应错误状态码。已成功发送的终止操作不会回滚，最终状态仍需查询确认。

## 兼容的 RPC 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/submit_job` | 按 Job 名称提交，成功返回 `200` 和 Run ID |
| `GET` | `/api/v1/get_run_status` | 通过查询参数 `run_id` 获取状态 |
| `POST` | `/api/v1/terminate_run` | 终止单个运行 |
| `POST` | `/api/v1/terminate_runs` | 原批量终止接口，部分成功仍返回 `409` |
| `POST` | `/api/v1/reload_location` | 重载代码位置 |
| `POST` | `/api/v1/shutdown_location` | 关闭代码位置；上游 SDK 已弃用该能力 |

这些路径保留既有字段和主要成功响应。原 `submit_job` 不要求幂等键，每次成功调用均可创建新 Run；需要防止重复触发时使用新的 `/api/v1/runs` 接口。代码位置操作检查 SDK 的操作状态，上游返回失败时不会报告 `reloaded`／`shutdown` 成功。

原 RPC 提交示例：

```bash
curl --fail-with-body -X POST http://localhost:8080/api/v1/submit_job \
  -H "Authorization: Bearer $APP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"repository_location_name":"example_location","job_name":"example_job","run_config":{},"tags":{"source":"webhook"}}'
```

`repository_name` 默认使用启动环境中的 `DAGSTER_REPOSITORY_NAME`，未配置时为 `__repository__`，通常无需修改。省略该字段或传入 `null` 都使用此默认值，空字符串仍返回 `422`。特殊情况下可在请求中显式传入其他 `repository_name`，其优先级高于环境变量。未指定代码位置时，网关仅在选定 Repository 内查找该 Job：无匹配返回 `404`，多个位置匹配返回 `409 JOB_SELECTION_AMBIGUOUS` 并附候选位置，调用方需指定 `repository_location_name`。网关不会用其他 Repository 覆盖请求值或默认值。

例如，整个网关主要连接一个使用自定义 Repository 的部署，可以设置 `DAGSTER_REPOSITORY_NAME=my_repository`；只有个别请求不同，则直接在请求中覆盖。该默认值只作用于旧的 `/api/v1/submit_job`；新的 `job_id` 资源 API 始终使用 ID 编码的实际 Repository，不会被默认值覆盖。

可通过 `op_selection` 选择 op，通过 `asset_selection` 选择资产；分层资产键可表示为字符串数组，例如 `[["warehouse","orders"]]`。

## 错误与请求追踪

| HTTP 状态码 | 含义 |
| --- | --- |
| `401` | 缺少、格式错误或无效的 Bearer Token |
| `403` | 调用方权限不足，或 Dagster 拒绝操作 |
| `404` | Job、Run 或代码位置不存在 |
| `409` | 幂等冲突、提交结果尚不确定、运行状态冲突或不支持的操作 |
| `422` | 请求字段、参数或 Dagster 运行配置无效 |
| `502` | Dagster 连接、服务或代码加载失败 |
| `503` | readiness 上游不可服务，或提交占位前持久化存储不可用 |
| `500` | 未预期的网关内部错误 |

错误响应增加稳定的 `code`、`request_id`、`retryable` 和结构化 `details`；原有 `detail`／`error_type` 等字段继续用于兼容及诊断。响应头 `X-Request-ID` 与错误体中的 `request_id` 一致，调用方可保存它以关联一次请求；可通过请求头传入由字母、数字、点、下划线、短横线组成的 1–128 字符 ID，否则网关自动生成。`retryable` 表示可按接口语义重试，提交操作仍须保持相同幂等键，不表示可以盲目创建新的业务请求。

关闭代码位置的 SDK 失败结果不区分位置不存在与服务错误，该接口失败统一返回 `502`。原批量终止部分成功返回 `409`，新逐项批量接口通过每项结果表达失败。

位置重载／关闭操作保留旧的 `message`、`detail`，并提供 `location_name`、`status`、`failure_type`。成功为 `SUCCESS` 且 `failure_type=null`；上游明确失败为 `FAILURE`；传输异常导致结果无法确认时为 `UNKNOWN`。网关鉴权／权限失败发生在执行操作之前，返回通用错误，不声称位置操作已发生。

现有异常日志继续原样记录上游错误；任务配置中的敏感值可能随错误进入日志。具体响应结构、字段约束和示例以 ReDoc 为准。

## 版本、测试与发布

版本格式为 `<Dagster 主版本.次版本.补丁版本>.<私有修订号>`。`1.13.26.4` 表示依赖 Dagster `1.13.26` 的第 4 个网关修订。升级 Dagster 时同步更新前三段，仅修改网关时递增最后一段。HTTP API 的 `/api/v1` 契约与包版本独立演进。

`pyproject.toml` 是网关版本的唯一来源，健康检查、OpenAPI 和 CI 发布使用相同版本。CI 校验版本与固定 Dagster 依赖的对应关系；已有版本标签属于其他提交时拒绝发布。

本地测试使用 SDK Mock、SQLite、临时本地服务及 GraphQL Schema 契约检查，不要求连接实际 Dagster 部署。测试通过不等于已完成实际部署联调。

```bash
uv pip install pytest httpx
uv run --no-sync python -m pytest -q
```

另有真实 Dagster `1.13.26` 的[隔离验收流程](integration/README.md)，包含并发提交、接受后响应丢失、重启恢复和批量终止。它只操作自己创建的测试实例，不接收业务服务地址。无 Docker 时可运行原生模式；原生模式通过不代表成品镜像已通过验收。

CI 先运行测试、构建成品镜像并在隔离 Dagster 环境中验收。只有 `master` 推送且验收通过后，才将已测过的同一个镜像发布到 GHCR 和 Docker Hub，使用 `latest` 和完整版本标签，并创建 `v<版本号>` Git 标签。PR 只测试，不发布。仓库 Secrets 需要配置 Docker Hub 凭据，钉钉通知使用可选的 `WEBHOOK_URL`。
