"""HTTP RPC gateway for the Dagster GraphQL API."""

import datetime
import logging
import os
import re
import secrets
import sqlite3
import uuid
from functools import lru_cache
from typing import Annotated, Any, Literal

import uvicorn
from dagster import DagsterRunStatus
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from dagster_graphql.client.utils import (
    ReloadRepositoryLocationStatus,
    ShutdownRepositoryLocationStatus,
)
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from starlette.exceptions import HTTPException as StarletteHTTPException

from gateway.dagster_adapter import DagsterAdapter
from gateway.discovery import create_discovery_router
from gateway.errors import ErrorResponse, GatewayHTTPError
from gateway.idempotency import IdempotencyStore
from gateway.job_resolution import resolve_job_location
from gateway.operations import create_operations_router
from gateway.security import Principal, authenticate, parse_api_keys
from gateway.settings import ConnectionSettings
from gateway.validation import validate_submission_tags
from project_metadata import VERSION

DAGSTER_HOST = "webserver"
DAGSTER_PORT = 3000
DAGSTER_TIMEOUT = 120
DEFAULT_REPOSITORY_NAME = os.getenv("DAGSTER_REPOSITORY_NAME", "__repository__").strip()
if not DEFAULT_REPOSITORY_NAME:
    raise ValueError("DAGSTER_REPOSITORY_NAME must be a non-empty Repository name")
ConnectionSettings.from_env()  # Reject malformed upstream configuration at startup.
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8080"))
API_TOKEN = os.getenv("APP_API_TOKEN") or secrets.token_urlsafe(32)
API_KEYS = parse_api_keys(os.getenv("APP_API_KEYS_JSON"))
LEGACY_TOKEN_ENABLED = bool(os.getenv("APP_API_TOKEN")) or not API_KEYS
if LEGACY_TOKEN_ENABLED and any(
    secrets.compare_digest(API_TOKEN.encode(), key.token.encode()) for key in API_KEYS
):
    raise ValueError("APP_API_TOKEN must differ from per-caller API key tokens")

logger = logging.getLogger("dagster_api_gateway")
logger.propagate = False
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(levelname)s:\t\t%(asctime)s - %(message)s")
    )
    logger.addHandler(handler)
if LEGACY_TOKEN_ENABLED and not os.getenv("APP_API_TOKEN"):
    logger.warning("!!! No APP_API_TOKEN set. Using ephemeral token: %s", API_TOKEN)


app = FastAPI(
    title="Dagster RPC/Webhook Gateway",
    version=VERSION,
    summary="通过带 Bearer Token 鉴权的 HTTP 接口操作 Dagster。",
    description=f"""
为外部系统、Webhook 和自动化工具提供 Dagster Job 提交、Run 查询和终止、代码位置管理接口。

## 鉴权
除 `/health`、`/ready` 和文档外，所有接口均要求请求头 `Authorization: Bearer <APP_API_TOKEN>`。
在网关环境变量中设置固定的 `APP_API_TOKEN`，或用 `APP_API_KEYS_JSON` 为各调用方分配
`read`、`submit`、`cancel`、`admin` 权限。`admin` 包含所有操作，旧 Token 保留全部权限。
两种配置均未设置时，生成当前进程使用的临时 Token。

## 请求与执行
POST 接口接收 `application/json`，GET 查询通过 URL 参数传入。
提交 Job 返回 `run_id`，表示 Dagster 已接收运行请求；执行进度通过状态查询接口获取。
终止接口返回终止请求的结果，最终运行状态仍以 Dagster 为准。

## 错误
`401` 表示网关鉴权失败；`403` 表示调用方权限不足或 Dagster 拒绝操作；`404` 表示资源不存在；
`409` 表示幂等冲突、提交结果不确定、运行状态冲突、原 RPC 批量终止失败或操作不受支持；
`422` 表示参数或运行配置无效；`502` 表示 Dagster 连接或服务错误；
`503` 表示上游未就绪或本地持久化存储不可用；`500` 表示网关内部错误。
Dagster 错误响应包含 `message`、`detail` 和 `error_type`。
所有错误另提供 `code`、`request_id`、`retryable` 和 `details`，请求标识也通过
`X-Request-ID` 响应头返回。外层请求字段严格校验，拼错字段返回 `422`。

## 资源 API
先通过 `/api/v1/jobs` 发现 Job 并取得复合 `job_id`，再获取配置 Schema 或执行只读验证。
`POST /api/v1/runs/submit` 按 `job_id` 非幂等提交，每次成功请求创建新 Run，返回 `201`。
`POST /api/v1/runs` 按调用方和 Key 去重，Key 可通过 `Idempotency-Key` 请求头或 Body 顶层
`idempotency_key` 提供，至少一处必填；两处相同可以接受，不同则返回 `422`。
Key 的传递位置不影响任务指纹，切换 Header 和 Body 后仍可重放原请求。
幂等接口首次创建返回 `201`，已接受请求重放返回 `200`；缺少 Key 不会降级为非幂等提交。
`409` 的结果不确定状态必须继续使用相同 Key 查询，不能换 Key 盲目重提。
原有 `POST /api/v1/submit_job` 按 Job 名称非幂等提交，成功仍返回 `200`。
提交接口均需要 `submit` 权限；非幂等请求超时后应先核查 Dagster，重试可能重复创建任务。
旧提交接口的 `repository_name` 省略或为 null 时，使用 `DAGSTER_REPOSITORY_NAME`
配置的默认值（默认 `__repository__`）；请求中显式指定的名称优先。
新资源接口的 `job_id` 始终携带实际 Repository，不受该默认值覆盖。

版本采用 `<Dagster 版本>.<网关修订号>`，例如 `{VERSION}`。
""",
    contact={
        "name": "项目问题反馈",
        "url": "https://github.com/sqkkyzx/dagster-webhook/issues",
    },
    openapi_tags=[
        {"name": "Monitoring", "description": "网关自身的存活状态与版本信息。"},
        {"name": "Jobs", "description": "向 Dagster 提交 Job 执行请求。"},
        {"name": "Runs", "description": "查询运行状态，发送单个或批量终止请求。"},
        {
            "name": "Submissions",
            "description": "查询本调用方的持久化提交记录，核对结果不确定的请求；不会重新创建 Run。",
        },
        {
            "name": "Locations",
            "description": "重载或关闭 Dagster Repository Location。",
        },
    ],
    openapi_external_docs={
        "description": "部署、环境变量与调用示例",
        "url": "https://github.com/sqkkyzx/dagster-webhook#readme",
    },
)


NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
AssetPath = Annotated[list[NonEmptyString], Field(min_length=1)]
RUN_ID_EXAMPLE = "12345678-1234-1234-1234-123456789abc"


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def finite_values(self):
        import json

        try:
            json.dumps(self.model_dump(), allow_nan=False)
        except (ValueError, TypeError):
            raise ValueError("Request values must be finite JSON") from None
        return self


class SubmitJobRequest(RequestModel):
    """提交运行时的作业选择、运行配置和执行范围。"""

    job_name: NonEmptyString = Field(
        description="Dagster Job 名称。", max_length=100, examples=["daily_job"]
    )
    repository_location_name: NonEmptyString | None = Field(
        None,
        description="代码位置名称。省略时仅在指定或默认 Repository 内查找 Job；多个位置匹配返回 409，需显式指定。",
        examples=["my_code"],
    )
    repository_name: NonEmptyString | None = Field(
        DEFAULT_REPOSITORY_NAME,
        description="Repository 名称。省略或为 null 时使用 DAGSTER_REPOSITORY_NAME 配置（默认 __repository__）；显式名称优先。",
        examples=[DEFAULT_REPOSITORY_NAME],
    )
    run_config: dict[str, Any] | None = Field(
        None,
        description="符合目标 Job 配置 Schema 的运行配置；省略时使用空配置。",
        examples=[{}],
    )
    tags: dict[str, Any] | None = Field(
        None,
        description="附加到 Run 的标签；非字符串值由 Dagster 规范化。禁止设置保留标签 .dagster/repository 和 dagster-webhook/submission_id。",
        examples=[{"source": "webhook"}],
    )
    op_selection: list[NonEmptyString] | None = Field(
        None,
        description="仅执行指定 op，使用 Dagster op 选择语法。省略时不限制执行范围。",
        examples=[["extract", "transform"]],
    )
    asset_selection: list[NonEmptyString | AssetPath] | None = Field(
        None,
        description="资产键列表：字符串表示单段键，字符串数组表示多段路径。",
        examples=[["customers", ["warehouse", "orders"]]],
    )
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "job_name": "daily_job",
                    "run_config": {},
                    "tags": {"source": "webhook"},
                }
            ]
        }
    )

    @field_validator("repository_name", mode="before")
    @classmethod
    def default_repository(cls, value):
        return DEFAULT_REPOSITORY_NAME if value is None else value

    @field_validator("tags")
    @classmethod
    def protected_tags(cls, value):
        return validate_submission_tags(value)


class RunActionRequest(RequestModel):
    run_id: NonEmptyString = Field(
        description="Dagster 返回的 Run ID。", examples=[RUN_ID_EXAMPLE]
    )


class TerminateRunsRequest(RequestModel):
    run_ids: list[NonEmptyString] = Field(
        min_length=1,
        max_length=200,
        description="要终止的 Run ID 列表，一次最多 200 个。",
        examples=[[RUN_ID_EXAMPLE]],
    )


class LocationActionRequest(RequestModel):
    location_name: NonEmptyString = Field(
        description="Dagster Workspace 中的代码位置名称。", examples=["my_code"]
    )


class HealthResponse(BaseModel):
    status: Literal["healthy"] = Field(description="网关进程状态。")
    service: str = Field(description="服务标识。", examples=["dagster-webhook-gateway"])
    timestamp: datetime.datetime = Field(description="带 UTC 时区的检查时间。")
    version: str = Field(description="与项目及镜像一致的网关版本。", examples=[VERSION])


class RunActionResponse(BaseModel):
    message: str = Field(description="请求处理结果。", examples=["success"])
    run_id: str = Field(
        description="本次操作对应的 Run ID。", examples=[RUN_ID_EXAMPLE]
    )


class RunStatusResponse(BaseModel):
    run_id: str = Field(description="查询的 Run ID。", examples=[RUN_ID_EXAMPLE])
    status: DagsterRunStatus = Field(
        description="Dagster 当前运行状态；SUCCESS、FAILURE、CANCELED 为终态。"
    )


class TerminateRunsResponse(BaseModel):
    message: str = Field(
        description="批量终止请求的处理结果。", examples=["batch terminate signal sent"]
    )
    run_ids: list[str] = Field(
        description="本次请求的 Run ID 列表。", examples=[[RUN_ID_EXAMPLE]]
    )


class LocationActionResponse(BaseModel):
    location_name: str = Field(
        description="本次操作对应的代码位置。", examples=["my_code"]
    )
    message: str = Field(description="操作结果。", examples=["reloaded"])
    status: Literal["SUCCESS"] = Field(description="Dagster 确认的代码位置操作状态。")
    detail: str = Field(description="Dagster SDK 返回的结果详情。")
    failure_type: str | None = Field(
        None, description="成功时为空；失败响应提供独立的失败类型。"
    )


class DagsterErrorResponse(ErrorResponse):
    message: str = Field(description="错误来源。", examples=["Dagster Client Error"])
    detail: str = Field(description="Dagster 返回的错误详情。")
    error_type: str = Field(
        description="Dagster 错误类型，用于调用方区分失败原因。",
        examples=["RunNotFoundError"],
    )


class LocationOperationErrorResponse(DagsterErrorResponse):
    location_name: str = Field(
        description="本次操作针对的代码位置。", examples=["my_code"]
    )
    status: Literal["FAILURE", "UNKNOWN"] = Field(
        description="FAILURE 表示上游明确拒绝或加载失败；UNKNOWN 表示传输异常等导致操作结果无法确认。"
    )
    failure_type: str = Field(description="可直接读取的失败类型，与 error_type 一致。")


class LocationOperationError(DagsterGraphQLClientError):
    def __init__(
        self, location_name: str, result_status: str, error: DagsterGraphQLClientError
    ):
        super().__init__(*error.args)
        self.location_name = location_name
        self.result_status = result_status


class AuthenticationErrorResponse(ErrorResponse):
    detail: str = Field(
        description="鉴权失败原因。", examples=["Invalid or missing API Token"]
    )


class ValidationIssue(BaseModel):
    loc: list[str | int] = Field(description="参数位置，例如 body、run_ids、0。")
    msg: str = Field(description="验证失败原因。")
    type: str = Field(description="参数验证错误类型。")
    model_config = ConfigDict(extra="allow")


class RequestValidationErrorResponse(ErrorResponse):
    detail: list[ValidationIssue] = Field(description="请求参数验证错误列表。")


ERROR_RESPONSES = {
    401: {
        "model": AuthenticationErrorResponse,
        "description": "缺少或无效的 Bearer Token。",
        "headers": {
            "WWW-Authenticate": {"schema": {"type": "string"}, "description": "Bearer"}
        },
    },
    403: {"model": ErrorResponse, "description": "网关权限不足或 Dagster 拒绝此操作。"},
    404: {"model": ErrorResponse, "description": "Job、Run 或代码位置不存在。"},
    409: {
        "model": ErrorResponse,
        "description": "运行状态冲突、Job 位置不唯一、终止失败或操作不受支持。",
    },
    422: {
        "model": RequestValidationErrorResponse | DagsterErrorResponse,
        "description": "请求参数或 Job 运行配置无效。",
    },
    502: {
        "model": ErrorResponse,
        "description": "Dagster 不可达或返回服务错误。",
    },
    500: {"model": ErrorResponse, "description": "网关内部错误。"},
}

# Response examples complement the schemas in ReDoc and Swagger UI.
for error_code, example_type, example_detail in (
    (403, "UnauthorizedError", "Dagster denied this operation"),
    (404, "RunNotFoundError", "Run ID not found"),
    (409, "TerminateRunsError", "Some runs could not be terminated"),
    (502, "DagsterGraphQLClientError", "Error when connecting to Dagster"),
):
    ERROR_RESPONSES[error_code]["content"] = {
        "application/json": {
            "example": {
                "message": "Dagster Client Error",
                "detail": example_detail,
                "error_type": example_type,
            }
        }
    }
ERROR_RESPONSES[401]["content"] = {
    "application/json": {"example": {"detail": "Invalid or missing API Token"}}
}
ERROR_RESPONSES[422]["content"] = {
    "application/json": {
        "examples": {
            "request_validation": {
                "summary": "缺少必填字段",
                "value": {
                    "detail": [
                        {
                            "loc": ["body", "job_name"],
                            "msg": "Field required",
                            "type": "missing",
                        }
                    ]
                },
            },
            "run_config_validation": {
                "summary": "Job 运行配置无效",
                "value": {
                    "message": "Dagster Client Error",
                    "detail": "Run config does not match the job schema",
                    "error_type": "RunConfigValidationInvalid",
                },
            },
        }
    }
}


def rpc_responses(description: str, example: dict[str, Any]) -> dict:
    for status_code, response in ERROR_RESPONSES.items():
        media = response.get("content", {}).get("application/json", {})
        examples = [media["example"]] if "example" in media else []
        examples += [entry["value"] for entry in media.get("examples", {}).values()]
        for value in examples:
            value.update(
                code={
                    401: "UNAUTHENTICATED",
                    403: "FORBIDDEN",
                    404: "RESOURCE_NOT_FOUND",
                    409: "OPERATION_CONFLICT",
                    422: "REQUEST_VALIDATION_FAILED",
                    502: "DAGSTER_UNAVAILABLE",
                }.get(status_code, "INTERNAL_ERROR"),
                request_id="example-request",
                retryable=False,
                details=[],
            )
    return {
        **ERROR_RESPONSES,
        200: {
            "description": description,
            "content": {"application/json": {"example": example}},
        },
    }


def location_responses(description: str, example: dict[str, Any]) -> dict:
    import copy

    result = copy.deepcopy(
        rpc_responses(description, {**example, "failure_type": None})
    )
    for code in (403, 404, 409, 502):
        result[code]["model"] = LocationOperationErrorResponse | ErrorResponse
        if code == 403:
            continue  # Gateway permission rejection occurs before an operation.
        failure_type = {
            404: "RepositoryLocationNotFound",
            409: "ReloadNotSupported",
            502: "PythonError",
        }[code]
        result[code]["content"]["application/json"]["example"].update(
            location_name="my_code",
            status="FAILURE",
            failure_type=failure_type,
            error_type=failure_type,
        )
    result[502]["description"] = (
        "上游明确失败或操作结果无法确认；通过 status 区分 FAILURE 和 UNKNOWN。"
    )
    return result


security = HTTPBearer(
    auto_error=False,
    scheme_name="GatewayBearerToken",
    description="使用 APP_API_TOKEN 或 APP_API_KEYS_JSON 中的 Token：Authorization: Bearer <token>。",
)


def verify_token(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
):
    principal = (
        authenticate(
            credentials.credentials,
            API_KEYS,
            API_TOKEN if LEGACY_TOKEN_ENABLED else None,
        )
        if credentials
        else None
    )
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


def require_scope(scope: str):
    def check(principal: Annotated[Principal, Depends(verify_token)]) -> Principal:
        if scope not in principal.scopes and "admin" not in principal.scopes:
            raise GatewayHTTPError(
                403, "FORBIDDEN", f"This operation requires the {scope} scope"
            )
        return principal

    check.required_scope = scope
    return check


def get_dagster_client() -> DagsterGraphQLClient:
    """A gql transport owns mutable connection state, so each request gets its own client."""
    return ConnectionSettings.from_env(
        fallback_host=DAGSTER_HOST,
        fallback_port=DAGSTER_PORT,
        fallback_timeout=DAGSTER_TIMEOUT,
    ).create_client()


ClientDependency = Annotated[DagsterGraphQLClient, Depends(get_dagster_client)]


def get_adapter(client: ClientDependency) -> DagsterAdapter:
    return DagsterAdapter(client)


@lru_cache(maxsize=8)
def _idempotency_store(path: str, retention: int, lease: int) -> IdempotencyStore:
    return IdempotencyStore(path, retention_seconds=retention, lease_seconds=lease)


def get_idempotency_store() -> IdempotencyStore:
    try:
        return _idempotency_store(
            os.getenv("IDEMPOTENCY_DB_PATH", "./data/idempotency.sqlite3"),
            int(os.getenv("IDEMPOTENCY_RETENTION_SECONDS", "86400")),
            int(os.getenv("IDEMPOTENCY_LEASE_SECONDS", "30")),
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        raise GatewayHTTPError(
            503, "PERSISTENCE_UNAVAILABLE", "Idempotency storage is unavailable"
        ) from exc


def get_readiness_client():
    settings = ConnectionSettings.from_env(
        fallback_host=DAGSTER_HOST,
        fallback_port=DAGSTER_PORT,
        fallback_timeout=DAGSTER_TIMEOUT,
    )
    return settings.create_client(timeout_override=settings.readiness_timeout)


def dagster_error_status(error_type: str) -> int:
    if error_type in {
        "JobNotFoundError",
        "PipelineNotFoundError",
        "RunNotFoundError",
        "PipelineRunNotFoundError",
        "RepositoryLocationNotFound",
        "RepositoryNotFoundError",
        "ModeNotFoundError",
        "PresetNotFoundError",
    }:
        return 404
    if error_type in {
        "RunConfigValidationInvalid",
        "PipelineConfigValidationInvalid",
        "JobConfigValidationInvalid",
        "PipelineConfigurationInvalid",
        "InvalidStepError",
        "InvalidOutputError",
        "ConflictingExecutionParamsError",
        "InvalidPipelineRunsFilterError",
        "InvalidSubsetError",
        "NoModeProvidedError",
    }:
        return 422
    if error_type in {
        "RunConflict",
        "TerminateRunFailure",
        "TerminateRunsError",
        "ReloadNotSupported",
    }:
        return 409
    if error_type == "UnauthorizedError":
        return 403
    return 502


HTTP_ERROR_CODES = {
    401: "UNAUTHENTICATED",
    403: "FORBIDDEN",
    404: "RESOURCE_NOT_FOUND",
    409: "OPERATION_CONFLICT",
    422: "REQUEST_VALIDATION_FAILED",
    502: "DAGSTER_UNAVAILABLE",
    503: "NOT_READY",
}


def error_fields(request: Request, status_code: int) -> dict:
    return {
        "code": HTTP_ERROR_CODES.get(status_code, "INTERNAL_ERROR"),
        "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
        "retryable": request.method == "GET" and status_code in {502, 503, 504},
        "details": [],
    }


@app.middleware("http")
async def request_identity(request: Request, call_next):
    supplied = request.headers.get("X-Request-ID", "")
    request.state.request_id = (
        supplied
        if re.fullmatch(r"[A-Za-z0-9._-]{1,128}", supplied)
        else str(uuid.uuid4())
    )
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    return response


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    fields = error_fields(request, exc.status_code)
    if isinstance(exc, GatewayHTTPError):
        fields.update(code=exc.code, retryable=exc.retryable, details=exc.details)
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, **fields},
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Field paths and messages are enough; arbitrary JSON inputs may contain NaN.
    errors = jsonable_encoder(
        [
            {k: v for k, v in e.items() if k not in {"input", "ctx"}}
            for e in exc.errors()
        ]
    )
    fields = error_fields(request, 422)
    fields["details"] = [
        {"path": e["loc"], "message": e["msg"], "code": e["type"]} for e in errors
    ]
    return JSONResponse(status_code=422, content={"detail": errors, **fields})


@app.exception_handler(Exception)
async def internal_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled gateway error, request_id=%s", request.state.request_id)
    fields = error_fields(request, 500)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal gateway error", **fields},
        headers={"X-Request-ID": fields["request_id"]},
    )


@app.exception_handler(DagsterGraphQLClientError)
def dagster_exception_handler(request: Request, exc: DagsterGraphQLClientError):
    # Keep the existing upstream error detail and logging behavior.
    logger.error(f"Dagster GraphQL Error: {exc}")
    error_type = str(exc.args[0]) if exc.args else "DagsterGraphQLClientError"
    # Transport errors contain free-form messages rather than a GraphQL error typename.
    if not error_type.isidentifier():
        error_type = "DagsterGraphQLClientError"
    status_code = dagster_error_status(error_type)
    fields = error_fields(request, status_code)
    if isinstance(exc, LocationOperationError):
        fields.update(
            location_name=exc.location_name,
            status=exc.result_status,
            failure_type=error_type,
        )
    if status_code == 422 and len(exc.args) > 1 and isinstance(exc.args[1], list):
        fields["details"] = [
            {
                "path": item.get("path", []),
                "message": item.get("message", "Invalid run configuration"),
                "code": item.get("reason", error_type),
            }
            for item in exc.args[1]
            if isinstance(item, dict)
        ]
    submission_id = getattr(exc, "gateway_submission_id", None)
    run_id = getattr(exc, "gateway_run_id", None)
    if submission_id:
        fields["details"].append(
            {
                "run_id": run_id,
                "submission_id": submission_id,
                "submission_url": f"/api/v1/submissions/{submission_id}",
            }
        )
    return JSONResponse(
        status_code=status_code,
        content={
            "message": "Dagster Client Error",
            "detail": str(exc),
            "error_type": error_type,
            **fields,
        },
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="网关健康检查",
    tags=["Monitoring"],
    operation_id="health_check",
)
async def health_check():
    """返回网关进程状态和版本，无需鉴权。此检查不探测 Dagster 的连接或运行状态。"""
    return {
        "status": "healthy",
        "service": "dagster-webhook-gateway",
        "timestamp": datetime.datetime.now(datetime.UTC),
        "version": VERSION,
    }


@app.post(
    "/api/v1/submit_job",
    response_model=RunActionResponse,
    summary="按 Job 名称提交（非幂等）",
    tags=["Jobs"],
    operation_id="submit_job",
    dependencies=[Depends(require_scope("submit"))],
    responses=rpc_responses(
        "Dagster 已接收 Job 执行请求。",
        {"message": "success", "run_id": RUN_ID_EXAMPLE},
    ),
)
def submit_job(payload: SubmitJobRequest, client: ClientDependency):
    """按 Job 名称非幂等提交一次执行，成功返回 200 和 Run ID。

    不要求幂等键，每次成功调用都会创建新 Run。需要使用复合 job_id 直接提交时，
    可选择 POST /api/v1/runs/submit；需要幂等控制时使用 POST /api/v1/runs。
    存在多个同名 Job 时需指定代码位置。

    Repository 省略或为 null 时使用网关默认配置，默认名称为 __repository__。
    可以通过 DAGSTER_REPOSITORY_NAME 修改默认值，或在本次请求中覆盖 repository_name。
    未指定代码位置时，网关仅在选定 Repository 内按 Job 名称查找位置；
    无匹配返回 404，多个位置匹配返回 409 JOB_SELECTION_AMBIGUOUS，并提供候选位置。
    Repository 不会被自动推断覆盖。禁止用户传入保留来源标签 .dagster/repository
    和提交关联标签 dagster-webhook/submission_id。

    每次成功调用都会提交新的 Run；请求超时后，应先核查 Dagster，再决定是否重新提交。
    """
    data = payload.model_dump()
    if data["repository_location_name"] is None:
        data["repository_location_name"] = resolve_job_location(
            DagsterAdapter(client), payload.job_name, payload.repository_name
        )
    run_id = DagsterAdapter(client).submit_legacy_job(data)
    logger.info("Submitted Job [%s] -> Run ID: %s", payload.job_name, run_id)
    return {"message": "success", "run_id": run_id}


@app.get(
    "/api/v1/get_run_status",
    response_model=RunStatusResponse,
    summary="获取 Run 状态",
    tags=["Runs"],
    operation_id="get_run_status",
    dependencies=[Depends(require_scope("read"))],
    responses=rpc_responses(
        "Run 当前状态。", {"run_id": RUN_ID_EXAMPLE, "status": "SUCCESS"}
    ),
)
def get_run_status(
    client: ClientDependency,
    run_id: Annotated[
        NonEmptyString, Query(description="Dagster Run ID。", examples=[RUN_ID_EXAMPLE])
    ],
):
    """查询指定 Run 的当前状态。Run 不存在时返回 404。"""
    return {"run_id": run_id, "status": client.get_run_status(run_id).value}


@app.post(
    "/api/v1/terminate_run",
    response_model=RunActionResponse,
    summary="终止单个 Run",
    tags=["Runs"],
    operation_id="terminate_run",
    dependencies=[Depends(require_scope("cancel"))],
    responses=rpc_responses(
        "终止请求已发送。",
        {"message": "terminate signal sent", "run_id": RUN_ID_EXAMPLE},
    ),
)
def terminate_run(payload: RunActionRequest, client: ClientDependency):
    """请求终止一个 Run。返回后可通过状态查询确认最终结果；不可终止的状态返回 409。"""
    client.terminate_run(payload.run_id)
    logger.info("Terminated Run ID: %s", payload.run_id)
    return {"message": "terminate signal sent", "run_id": payload.run_id}


@app.post(
    "/api/v1/terminate_runs",
    response_model=TerminateRunsResponse,
    summary="批量终止 Runs",
    tags=["Runs"],
    operation_id="terminate_runs",
    dependencies=[Depends(require_scope("cancel"))],
    responses=rpc_responses(
        "全部 Run 的终止请求已发送。",
        {"message": "batch terminate signal sent", "run_ids": [RUN_ID_EXAMPLE]},
    ),
)
def terminate_runs(payload: TerminateRunsRequest, client: ClientDependency):
    """一次终止 1–200 个 Run。

    部分成功也会返回 409，已终止的 Run 不会回滚。请结合错误详情与各 Run 状态判断结果。
    """
    client.terminate_runs(payload.run_ids)
    logger.info("Terminated Batch Run IDs: %s", payload.run_ids)
    return {"message": "batch terminate signal sent", "run_ids": payload.run_ids}


@app.post(
    "/api/v1/reload_location",
    response_model=LocationActionResponse,
    summary="重载代码位置",
    tags=["Locations"],
    operation_id="reload_location",
    dependencies=[Depends(require_scope("admin"))],
    responses=location_responses(
        "代码位置已成功重载。",
        {
            "message": "reloaded",
            "location_name": "my_code",
            "status": "SUCCESS",
            "detail": "ReloadRepositoryLocationInfo(status=<ReloadRepositoryLocationStatus.SUCCESS: 'SUCCESS'>, failure_type=None, message=None)",
        },
    ),
)
def reload_location(payload: LocationActionRequest, client: ClientDependency):
    """重载指定 Repository Location，并检查 Dagster 返回的操作状态。

    位置不存在返回 404，不支持重载返回 409，代码加载失败返回 502。
    操作响应提供 location_name、status、failure_type；FAILURE 表示明确失败，
    UNKNOWN 表示未能确认操作结果。detail 保留旧版文本，调用方无需解析它来判断状态。
    """
    try:
        result = client.reload_repository_location(payload.location_name)
    except DagsterGraphQLClientError as exc:
        raise LocationOperationError(payload.location_name, "UNKNOWN", exc) from exc
    if result.status != ReloadRepositoryLocationStatus.SUCCESS:
        raise LocationOperationError(
            payload.location_name,
            "FAILURE",
            DagsterGraphQLClientError(
                result.failure_type or "PythonError", result.message
            ),
        )
    logger.info("Reloaded location: %s", payload.location_name)
    return {
        "message": "reloaded",
        "location_name": payload.location_name,
        "status": "SUCCESS",
        "detail": str(result),
        "failure_type": None,
    }


@app.post(
    "/api/v1/shutdown_location",
    response_model=LocationActionResponse,
    summary="关闭代码位置（已弃用）",
    tags=["Locations"],
    operation_id="shutdown_location",
    deprecated=True,
    dependencies=[Depends(require_scope("admin"))],
    responses=location_responses(
        "代码位置已成功关闭。",
        {
            "message": "shutdown",
            "location_name": "my_code",
            "status": "SUCCESS",
            "detail": "ShutdownRepositoryLocationInfo(status=<ShutdownRepositoryLocationStatus.SUCCESS: 'SUCCESS'>, message=None)",
        },
    ),
)
def shutdown_location(payload: LocationActionRequest, client: ClientDependency):
    """关闭提供代码位置元数据的服务。此 Dagster SDK 功能已弃用，保留供已有调用方使用。

    SDK 的失败结果不区分位置不存在与服务错误，统一返回 502，failure_type 为 PythonError。
    操作响应包含 location_name、status 和 failure_type；明确失败为 FAILURE，无法确认结果为 UNKNOWN。
    若部署平台配置自动重启，服务可能随后重新启动。
    """
    try:
        result = client.shutdown_repository_location(payload.location_name)
    except DagsterGraphQLClientError as exc:
        raise LocationOperationError(payload.location_name, "UNKNOWN", exc) from exc
    if result.status != ShutdownRepositoryLocationStatus.SUCCESS:
        raise LocationOperationError(
            payload.location_name,
            "FAILURE",
            DagsterGraphQLClientError("PythonError", result.message),
        )
    logger.info("Shut down location: %s", payload.location_name)
    return {
        "message": "shutdown",
        "location_name": payload.location_name,
        "status": "SUCCESS",
        "detail": str(result),
        "failure_type": None,
    }


class ReadinessResponse(BaseModel):
    status: Literal["ready"] = Field(
        description="Dagster GraphQL 服务可以响应短时探测。"
    )
    version: str = Field(description="当前网关版本。", examples=[VERSION])


@app.get(
    "/ready",
    response_model=ReadinessResponse,
    tags=["Monitoring"],
    operation_id="readiness_check",
    summary="检查上游连接是否就绪",
    responses={
        503: {"model": ErrorResponse, "description": "Dagster 上游不可达或响应异常。"}
    },
)
def readiness_check(
    client: Annotated[DagsterGraphQLClient, Depends(get_readiness_client)],
):
    """无需鉴权，用短超时执行只读 GraphQL 探测。成功返回 200，失败返回 503。

    此接口验证 GraphQL 服务可响应，不保证每个代码位置或 Job 已成功加载。
    DAGSTER_READINESS_TIMEOUT 默认 3 秒，与业务超时独立；/health 继续只检查进程存活。
    """
    try:
        result = client._execute("query GatewayReadiness { __typename }")
        if not result.get("__typename"):
            raise ValueError("Missing GraphQL response")
    except Exception as exc:
        raise GatewayHTTPError(
            503, "NOT_READY", "Dagster GraphQL is not ready", retryable=True
        ) from exc
    return {"status": "ready", "version": VERSION}


discovery_router = create_discovery_router(get_adapter, require_scope("read"))
operations_router = create_operations_router(
    get_adapter,
    get_idempotency_store,
    require_scope("submit"),
    require_scope("cancel"),
)


def _dependency_scopes(dependant):
    scopes = set()
    required = getattr(dependant.call, "required_scope", None)
    if required:
        scopes.add(required)
    for child in dependant.dependencies:
        scopes.update(_dependency_scopes(child))
    return scopes


for route in [*app.routes, *discovery_router.routes, *operations_router.routes]:
    if not isinstance(route, APIRoute):
        continue
    required = sorted(_dependency_scopes(route.dependant))
    if required:
        route.description += (
            "\n\n所需权限："
            + ", ".join(f"`{s}`" for s in required)
            + "（`admin` 或旧共享 Token 也可调用）。"
        )
        route.openapi_extra = {
            **(route.openapi_extra or {}),
            "x-required-scopes": required,
        }
    for response in route.responses.values():
        response.setdefault("headers", {})["X-Request-ID"] = {
            "description": "本次请求标识，与错误响应 request_id 一致。",
            "schema": {"type": "string"},
        }
    if route.path == "/api/v1/runs" and "POST" in route.methods:
        for status_code in (200, 201):
            route.responses[status_code].setdefault("headers", {}).update(
                {
                    "Location": {
                        "description": "对应 Run 详情的相对链接。",
                        "schema": {"type": "string"},
                    },
                    "Idempotency-Replayed": {
                        "description": "true 表示已接受请求重放，false 表示首次创建。",
                        "schema": {"type": "string"},
                    },
                }
            )
        route.responses[409].setdefault("headers", {})["Retry-After"] = {
            "description": "已有请求仍在处理中时建议等待的秒数；结果不确定时不一定提供。",
            "schema": {"type": "integer"},
        }


app.include_router(discovery_router)
app.include_router(operations_router)


if __name__ == "__main__":
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
