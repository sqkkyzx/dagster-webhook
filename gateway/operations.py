"""Durable run submission and structured batch operations."""

import datetime
import json
import logging
import sqlite3
from typing import Annotated, Any, Literal

from dagster import DagsterRunStatus
from dagster_graphql import DagsterGraphQLClientError
from fastapi import APIRouter, Depends, Header, Path, Response
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from gateway.errors import GatewayHTTPError, responses
from gateway.idempotency import (
    IdempotencyConflict,
    StoredSubmissionRejection,
    SubmissionNotFound,
)
from gateway.validation import validate_submission_tags

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
AssetPath = Annotated[list[NonEmpty], Field(min_length=1)]
IdempotencyKey = Annotated[
    str,
    StringConstraints(
        strict=True, min_length=1, max_length=200, pattern=r"^[A-Za-z0-9._:-]+$"
    ),
]


class RunSubmissionRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "job_id": "WyJteV9jb2RlIiwibXlfcmVwb3NpdG9yeSIsImRhaWx5X2pvYiJd",
                    "run_config": {},
                    "tags": {"source": "automation"},
                }
            ]
        },
    )
    job_id: NonEmpty = Field(
        description="从 Job 发现接口取得的复合 ID，包含代码位置、Repository 和 Job 名称。"
    )
    run_config: dict[str, Any] = Field(
        default_factory=dict,
        description="符合该 Job 配置要求的运行配置。",
        examples=[{}],
    )
    tags: dict[str, Any] = Field(
        default_factory=dict,
        description="附加运行标签；.dagster/repository 和 dagster-webhook/submission_id 是来源识别及恢复用的保留标签，不允许传入。",
        examples=[{"source": "automation"}],
    )
    op_selection: list[NonEmpty] | None = Field(
        None, description="可选 op 执行范围。", examples=[["extract"]]
    )
    asset_selection: list[NonEmpty | AssetPath] | None = Field(
        None,
        description="可选资产键；字符串为单段键，数组为多段路径。",
        examples=[[["warehouse", "orders"]]],
    )

    @field_validator("tags")
    @classmethod
    def protected_tags(cls, value):
        return validate_submission_tags(value)

    @model_validator(mode="after")
    def finite_json(self):
        try:
            json.dumps(self.model_dump(), allow_nan=False)
        except (ValueError, TypeError):
            raise ValueError(
                "run_config and tags must contain finite JSON values"
            ) from None
        return self


class IdempotentRunSubmissionRequest(RunSubmissionRequest):
    idempotency_key: IdempotencyKey | None = Field(
        None,
        description="调用方内唯一的幂等键，可替代 Header Idempotency-Key。Header 与 Body 至少提供一处；两处同值接受，不同返回 422。仅允许 1–200 个字母、数字、点、下划线、冒号和短横线；null 视为未提供，空字符串无效。",
        examples=["daily-import-2026-11-08"],
    )
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "job_id": "WyJteV9jb2RlIiwibXlfcmVwb3NpdG9yeSIsImRhaWx5X2pvYiJd",
                    "run_config": {},
                },
                {
                    "job_id": "WyJteV9jb2RlIiwibXlfcmVwb3NpdG9yeSIsImRhaWx5X2pvYiJd",
                    "run_config": {},
                    "idempotency_key": "daily-import-2026-11-08",
                },
            ]
        }
    )


class DirectRunResponse(BaseModel):
    run_id: str = Field(description="Dagster 创建并返回的实际 Run ID。")
    status: DagsterRunStatus = Field(
        description="提交时的运行状态快照；实时状态通过 status_url 查询。",
        examples=["QUEUED"],
    )
    job_id: str = Field(description="本次提交的复合 Job ID。")
    status_url: str = Field(description="对应 Run 详情的相对链接，读取需要 read 权限。")


class CreatedRunResponse(BaseModel):
    run_id: str = Field(description="Dagster 创建并返回的实际 Run ID。")
    status: DagsterRunStatus = Field(
        description="提交或恢复确认时的状态快照；实时状态请查询 Run 详情。",
        examples=["QUEUED"],
    )
    job_id: str = Field(description="本次请求的复合 Job ID。")
    submission_id: str = Field(description="持久化幂等记录的标识。")
    replayed: bool = Field(description="是否返回了同一幂等请求已接受的结果。")
    status_url: str = Field(description="相对于当前网关的 Run 详情链接。")
    submission_url: str | None = Field(
        None, description="本次持久化提交状态的相对链接，无需原请求体即可查询。"
    )


class SubmissionRunSnapshot(BaseModel):
    run_id: str = Field(description="Dagster Run ID。")
    status: DagsterRunStatus = Field(
        description="接受提交或恢复核对时的运行状态快照，不是实时状态。"
    )
    job_id: str | None = Field(
        None, description="复合 Job ID；早期记录可能没有此字段。"
    )
    job_name: str | None = Field(None, description="Job 名称；早期记录可能为空。")
    repository_location_name: str | None = Field(None, description="Run 来源代码位置。")
    repository_name: str | None = Field(None, description="Run 来源 Repository。")
    created_at: datetime.datetime | None = Field(
        None, description="Run 创建的 UTC 时间；早期快照可能为空。"
    )
    started_at: datetime.datetime | None = Field(
        None, description="Run 开始的 UTC 时间；未开始或早期快照可能为空。"
    )
    ended_at: datetime.datetime | None = Field(
        None, description="Run 结束的 UTC 时间；未结束或早期快照可能为空。"
    )
    parent_run_id: str | None = Field(None, description="父 Run ID（如适用）。")
    root_run_id: str | None = Field(None, description="运行链根 Run ID（如适用）。")


class SubmissionResponse(BaseModel):
    submission_id: str = Field(
        description="本地持久化提交标识；不等同于 Run ID。",
        examples=["example-submission"],
    )
    run_id: str | None = Field(
        None, description="Dagster 实际 Run ID；响应丢失且尚未核对到运行时为空。"
    )
    state: Literal["REQUESTING", "ACCEPTED", "REJECTED", "UNKNOWN"] = Field(
        description="REQUESTING 为有效租约内处理；ACCEPTED 为已确认接受；REJECTED 为明确拒绝；UNKNOWN 为仍无法确定。租约到期的 REQUESTING 在查询中呈现为 UNKNOWN。"
    )
    created_at: datetime.datetime = Field(description="提交占位记录创建的 UTC 时间。")
    expires_at: datetime.datetime | None = Field(
        None,
        description="已接受/明确拒绝记录的 UTC 保留截止时间；UNKNOWN 不自动过期。过期后查询返回 404。",
    )
    lease_expires_at: datetime.datetime | None = Field(
        None, description="REQUESTING 的 UTC 租约截止时间；其余状态为空。"
    )
    last_checked_at: datetime.datetime | None = Field(
        None,
        description="最近一次执行提交关联查询的 UTC 时间；新记录按恢复标记核对，历史记录兼容 Run ID，从未核对时为空。",
    )
    last_check_outcome: (
        Literal["ACCEPTED", "NOT_FOUND", "NOT_STARTED", "UPSTREAM_ERROR", "MISMATCH"]
        | None
    ) = Field(
        None,
        description="最近核对结果。NOT_FOUND/NOT_STARTED/UPSTREAM_ERROR/MISMATCH 均不能证明未执行，保持 UNKNOWN。MISMATCH 表示来源 Job 与提交记录不一致；UPSTREAM_ERROR 也包括标记匹配多条记录等上游异常。没有核对记录时为空。",
    )
    error_type: str | None = Field(
        None, description="明确拒绝时的上游错误类型；不保存原始错误或任务配置。"
    )
    run: SubmissionRunSnapshot | None = Field(
        None, description="已接受运行的安全字段快照；不包含原始配置、标签或凭据。"
    )
    status_url: str | None = Field(
        None,
        description="已知 Run 的详情链接；未知 Run ID 时为空。读取需要 read 权限。",
    )
    submission_url: str = Field(description="当前提交记录的查询链接。")


SUBMISSION_EXAMPLE = {
    "submission_id": "example-submission",
    "run_id": None,
    "state": "UNKNOWN",
    "created_at": "2026-11-08T00:00:00Z",
    "expires_at": None,
    "lease_expires_at": None,
    "last_checked_at": "2026-11-08T00:00:01Z",
    "last_check_outcome": "NOT_FOUND",
    "error_type": None,
    "run": None,
    "status_url": None,
    "submission_url": "/api/v1/submissions/example-submission",
}
SubmissionId = Annotated[
    str,
    Path(
        min_length=1,
        max_length=100,
        description="提交响应或不确定错误 details 中的 submission_id。",
        examples=["example-submission"],
    ),
]


def _lookup_submission_run(adapter, recovery_kind, token):
    try:
        if recovery_kind == "submission_tag":
            return adapter.find_submission_run(token)
        return adapter.get_run(token)
    except DagsterGraphQLClientError as exc:
        if exc.args and exc.args[0] in {"RunNotFoundError", "PipelineRunNotFoundError"}:
            return None
        raise


def _submission_response(record):
    return {
        **record,
        "status_url": f"/api/v1/runs/{record['run_id']}" if record["run_id"] else None,
        "submission_url": f"/api/v1/submissions/{record['submission_id']}",
    }


def _submission_error(exc):
    if isinstance(exc, SubmissionNotFound):
        return GatewayHTTPError(
            404, "RESOURCE_NOT_FOUND", "Submission not found or no longer retained"
        )
    if isinstance(exc, IdempotencyConflict):
        return GatewayHTTPError(
            409,
            exc.code,
            exc.message,
            details=[{"run_id": exc.run_id, "submission_id": exc.submission_id}],
            headers={"Retry-After": str(exc.retry_after)} if exc.retry_after else None,
            retryable=bool(exc.retry_after),
        )
    return GatewayHTTPError(
        503, "PERSISTENCE_UNAVAILABLE", "Idempotency storage is unavailable"
    )


class BatchTerminationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_ids: list[NonEmpty] = Field(
        min_length=1,
        max_length=200,
        description="1–200 个不同 Run ID。",
        examples=[["run-1", "run-2"]],
    )

    @model_validator(mode="after")
    def unique_ids(self):
        if len(set(self.run_ids)) != len(self.run_ids):
            raise ValueError("run_ids must not contain duplicates")
        return self


class TerminationResult(BaseModel):
    run_id: str = Field(description="对应的 Run ID。")
    status: Literal["accepted", "failed"] = Field(
        description="accepted 表示终止请求已接受，failed 表示该项失败。",
        examples=["accepted"],
    )
    error_type: str | None = Field(None, description="失败时的 Dagster 错误类型。")
    detail: str | None = Field(None, description="失败详情。")


class BatchTerminationResponse(BaseModel):
    results: list[TerminationResult] = Field(
        description="按请求顺序返回每个 Run 的终止结果。"
    )
    succeeded: int = Field(description="终止请求被接受的项数。")
    failed: int = Field(description="失败的项数。")


DEFINITE_REJECTIONS = {
    "JobNotFoundError",
    "PipelineNotFoundError",
    "RepositoryLocationNotFound",
    "RunConfigValidationInvalid",
    "PipelineConfigValidationInvalid",
    "JobConfigValidationInvalid",
    "InvalidStepError",
    "InvalidOutputError",
    "UnauthorizedError",
    "ConflictingExecutionParamsError",
    "InvalidSubsetError",
    "PresetNotFoundError",
    "NoModeProvidedError",
    "ModeNotFoundError",
}


def create_operations_router(get_adapter, get_store, require_submit, require_cancel):
    router = APIRouter(prefix="/api/v1")

    direct_responses = responses(
        "本次非幂等提交已被 Dagster 接受。每次成功调用创建新的 Run。",
        {
            "run_id": "example-run",
            "status": "QUEUED",
            "job_id": "example-job-id",
            "status_url": "/api/v1/runs/example-run",
        },
        201,
    )
    direct_responses.pop(503)  # This route does not depend on submission storage.
    direct_responses[201]["headers"] = {
        "Location": {
            "description": "对应 Run 详情的相对链接。",
            "schema": {"type": "string"},
        }
    }

    @router.post(
        "/runs/submit",
        status_code=201,
        response_model=DirectRunResponse,
        tags=["Runs"],
        operation_id="submit_run_without_idempotency",
        summary="直接提交 Run（非幂等）",
        dependencies=[Depends(require_submit)],
        responses=direct_responses,
    )
    def submit_run_direct(
        payload: RunSubmissionRequest,
        response: Response,
        adapter: Annotated[Any, Depends(get_adapter)],
    ):
        """按复合 job_id 直接提交一次 Run，不要求幂等键或幂等数据库。

        每次成功请求都会创建新的 Run，相同请求重复发送也会创建新运行。
        此接口不读取 Idempotency-Key Header；Body 不接受 idempotency_key 字段。
        需要防重时使用 POST /api/v1/runs，并从 Header 或 Body 提供幂等键。
        成功返回 201、Run ID、状态快照和 Location/status_url；没有 submission_id 或重放标记。
        超时或上游错误时任务可能已创建，返回错误不表示未执行；应先核查 Dagster 再决定重试。
        此接口不生成可供 submission 查询/核对的持久化记录。
        """
        run = adapter.submit_direct_run(payload.model_dump())
        response.headers["Location"] = f"/api/v1/runs/{run['run_id']}"
        return {
            "run_id": run["run_id"],
            "status": run["status"],
            "job_id": payload.job_id,
            "status_url": f"/api/v1/runs/{run['run_id']}",
        }

    @router.post(
        "/runs",
        status_code=201,
        response_model=CreatedRunResponse,
        tags=["Runs"],
        operation_id="create_run",
        summary="幂等提交一次 Run",
        responses={
            **responses(
                "首次创建 Run。",
                {
                    "run_id": "example-run",
                    "status": "QUEUED",
                    "job_id": "example-job-id",
                    "submission_id": "example-submission",
                    "submission_url": "/api/v1/submissions/example-submission",
                    "replayed": False,
                    "status_url": "/api/v1/runs/example-run",
                },
                201,
            ),
            200: {
                "model": CreatedRunResponse,
                "description": "返回同一幂等请求已接受的结果。",
                "headers": {
                    "Idempotency-Replayed": {
                        "schema": {"type": "string"},
                        "description": "true",
                    }
                },
            },
        },
    )
    def create_run(
        payload: IdempotentRunSubmissionRequest,
        response: Response,
        principal: Annotated[Any, Depends(require_submit)],
        adapter: Annotated[Any, Depends(get_adapter)],
        store: Annotated[Any, Depends(get_store)],
        key: Annotated[
            IdempotencyKey | None,
            Header(
                alias="Idempotency-Key",
                description="调用方内唯一的请求键，可与 Body idempotency_key 二选一。至少提供一处；两处同值接受，不同返回 422。只含 1–200 个字母、数字、点、下划线、冒号或短横线。",
                examples=["daily-import-2026-11-08"],
            ),
        ] = None,
    ):
        """同一调用方、相同 Key 和相同参数在保留期内只创建一个 Run。

        从 Header Idempotency-Key 或 Body idempotency_key 提供幂等键，至少一处必填。
        两处同值接受，不同返回 422 IDEMPOTENCY_KEY_MISMATCH；都没有时返回 422，不会降级为非幂等提交。
        所有已提供的值均需符合格式约束；Body null 视为未提供，空字符串或非法字符会被拒绝。
        Key 的传递位置不参与请求指纹，不会转发到 Dagster；同一请求可切换位置重试。
        首次创建返回 201；已接受请求重放返回 200。返回的 status 是快照，后续通过 status_url 查询。
        submission_url 可在不重传原配置的情况下查询本地提交状态；结果不确定时可按 submission_id 调用 reconcile 核对。
        Key 绑定的参数不同返回 409；处理中返回 409 和 Retry-After。
        提交前持久化独占请求记录与恢复标记。提交超时可能已创建 Run，再次使用同 Key 时仅按受保护标记核对，不自动重新提交。
        若只找到 NOT_STARTED 的 Run，尚无法证明 Dagster 已接受提交，继续返回结果不确定。
        SUBMISSION_OUTCOME_UNKNOWN 表示结果仍不确定，不能换 Key 盲目重提。
        默认已接受/明确拒绝记录保留 24 小时，过期后相同 Key 可以创建新 Run；不确定记录不自动过期。
        SQLite 仅支持同机持久卷，不支持跨主机共享；存储路径需在容器重建后保留。
        """
        if key is None and payload.idempotency_key is None:
            raise GatewayHTTPError(
                422,
                "REQUEST_VALIDATION_FAILED",
                "Provide Idempotency-Key in the header or idempotency_key in the body",
                details=[
                    {
                        "path": ["body", "idempotency_key"],
                        "message": "Provide a key here or in the Idempotency-Key header",
                        "code": "missing",
                    }
                ],
            )
        if (
            key is not None
            and payload.idempotency_key is not None
            and key != payload.idempotency_key
        ):
            raise GatewayHTTPError(
                422,
                "IDEMPOTENCY_KEY_MISMATCH",
                "Header and body idempotency keys must match",
                details=[
                    {
                        "path": ["body", "idempotency_key"],
                        "message": "Must match the Idempotency-Key header",
                        "code": "value_error",
                    }
                ],
            )
        key = key if key is not None else payload.idempotency_key
        data = payload.model_dump(exclude={"idempotency_key"})
        # Reject a malformed composite identity before occupying a durable key.
        from gateway.dagster_adapter import decode_job_id

        decode_job_id(payload.job_id)

        try:
            result = store.submit(
                principal.name,
                key,
                data,
                create=lambda token: adapter.submit_run(data, token),
                lookup=lambda kind, token: _lookup_submission_run(adapter, kind, token),
                is_rejection=lambda exc: (
                    isinstance(exc, DagsterGraphQLClientError)
                    and bool(exc.args)
                    and exc.args[0] in DEFINITE_REJECTIONS
                ),
            )
        except StoredSubmissionRejection as exc:
            rejection = DagsterGraphQLClientError(
                exc.error_type,
                "This idempotent submission was previously rejected; use a new key after correcting the request",
            )
            rejection.gateway_run_id = exc.run_id
            rejection.gateway_submission_id = exc.submission_id
            raise rejection from exc
        except DagsterGraphQLClientError as exc:
            if exc.args and exc.args[0] in DEFINITE_REJECTIONS:
                raise
            logging.getLogger("dagster_api_gateway").error(
                f"Dagster GraphQL Error: {exc}"
            )
            raise GatewayHTTPError(
                409,
                "SUBMISSION_OUTCOME_UNKNOWN",
                str(exc),
                details=[
                    {
                        "run_id": getattr(exc, "gateway_run_id", None),
                        "submission_id": getattr(exc, "gateway_submission_id", None),
                        "resolution": "Retry with the same idempotency key in the header or body to check the existing run; do not create a new key",
                    }
                ],
            ) from exc
        except IdempotencyConflict as exc:
            details = [{"run_id": exc.run_id, "submission_id": exc.submission_id}]
            headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after else None
            raise GatewayHTTPError(
                409,
                exc.code,
                exc.message,
                details=details,
                headers=headers,
                retryable=bool(exc.retry_after),
            ) from exc
        except Exception as exc:
            run_id = getattr(exc, "gateway_run_id", None)
            submission_id = getattr(exc, "gateway_submission_id", None)
            if submission_id:
                raise GatewayHTTPError(
                    409,
                    "SUBMISSION_OUTCOME_UNKNOWN",
                    "The submission result could not be confirmed; retry with the same idempotency key",
                    details=[{"run_id": run_id, "submission_id": submission_id}],
                ) from exc
            if isinstance(exc, sqlite3.Error):
                raise GatewayHTTPError(
                    503, "PERSISTENCE_UNAVAILABLE", "Idempotency storage is unavailable"
                ) from exc
            raise
        response.status_code = 200 if result.replayed else 201
        response.headers["Location"] = f"/api/v1/runs/{result.run['run_id']}"
        response.headers["Idempotency-Replayed"] = str(result.replayed).lower()
        return {
            "run_id": result.run["run_id"],
            "status": result.run["status"],
            "job_id": payload.job_id,
            "submission_id": result.submission_id,
            "replayed": result.replayed,
            "status_url": f"/api/v1/runs/{result.run['run_id']}",
            "submission_url": f"/api/v1/submissions/{result.submission_id}",
        }

    @router.get(
        "/submissions/{submission_id}",
        response_model=SubmissionResponse,
        tags=["Submissions"],
        operation_id="get_submission",
        summary="查询本调用方的提交状态",
        responses=responses(
            "返回本地持久化提交状态，不联系 Dagster。", SUBMISSION_EXAMPLE
        ),
    )
    def get_submission(
        submission_id: SubmissionId,
        principal: Annotated[Any, Depends(require_submit)],
        store: Annotated[Any, Depends(get_store)],
    ):
        """仅查询本地提交记录，无需重传 Idempotency-Key 或原任务配置，也不会联系 Dagster。

        需要 submit 权限，并且记录必须属于当前调用方 name；admin 也不会绕过调用方隔离。
        其他调用方、记录不存在、已接受或明确拒绝记录到期均返回相同的 404。
        UNKNOWN 不自动过期；查询不会续期、核对或再次创建任务。
        ACCEPTED 仅表示接受提交，run 是历史快照；实时运行状态应查询 status_url（需要 read 权限）。
        """
        try:
            return _submission_response(store.get(principal.name, submission_id))
        except (SubmissionNotFound, sqlite3.Error) as exc:
            raise _submission_error(exc) from exc

    reconcile_responses = responses(
        "返回核对后的提交状态；UNKNOWN 表示仍未确认，不是未执行。", SUBMISSION_EXAMPLE
    )
    reconcile_responses[409] = {
        **reconcile_responses[409],
        "description": "有效租约内仍在处理，或核对后的持久化失败，无法确认提交结果；错误 details 保留提交 ID 和已知的真实 Run ID。",
        "headers": {
            "Retry-After": {
                "description": "处理中建议等待的秒数。",
                "schema": {"type": "integer"},
            }
        },
    }

    @router.post(
        "/submissions/{submission_id}/reconcile",
        response_model=SubmissionResponse,
        tags=["Submissions"],
        operation_id="reconcile_submission",
        summary="核对本调用方已有提交的结果",
        responses=reconcile_responses,
    )
    def reconcile_submission(
        submission_id: SubmissionId,
        principal: Annotated[Any, Depends(require_submit)],
        store: Annotated[Any, Depends(get_store)],
        adapter: Annotated[Any, Depends(get_adapter)],
    ):
        """按持久化恢复标记查询 Dagster，并更新本地核对记录。历史记录兼容按 Run ID 核对。无需请求体或原始配置。

        不创建、重新提交、终止 Run，也不会释放幂等键。需要 submit 权限且属于当前调用方；越权与不存在统一返回 404。
        ACCEPTED/REJECTED 直接返回原记录，不再查询上游或延长保留期。
        有效 REQUESTING 租约内返回 409 IDEMPOTENCY_IN_PROGRESS 和 Retry-After。
        UNKNOWN 或租约到期时执行核对：唯一匹配且来源 Job 一致的已接受运行恢复为 ACCEPTED；查无运行、仅 NOT_STARTED、来源不匹配、匹配多条或上游异常仍为 UNKNOWN。恢复标记由网关生成，调用方不能通过 tags 设置。
        正常完成核对返回 200，请检查 state 和 last_check_outcome；200 不表示业务执行成功。
        核对后持久化失败返回 409 SUBMISSION_OUTCOME_UNKNOWN，details 保留提交 ID 和已知的真实 Run ID；存储读取不可用返回 503。
        """
        try:
            return _submission_response(
                store.reconcile(
                    principal.name,
                    submission_id,
                    lambda kind, token: _lookup_submission_run(adapter, kind, token),
                )
            )
        except (SubmissionNotFound, IdempotencyConflict, sqlite3.Error) as exc:
            raise _submission_error(exc) from exc

    @router.post(
        "/runs/terminate-batch",
        response_model=BatchTerminationResponse,
        tags=["Runs"],
        operation_id="terminate_runs_structured",
        summary="批量终止并返回逐项结果",
        dependencies=[Depends(require_cancel)],
        responses=responses(
            "已处理批量终止请求，包括部分失败。",
            {
                "results": [
                    {"run_id": "run-1", "status": "accepted"},
                    {
                        "run_id": "run-2",
                        "status": "failed",
                        "error_type": "RunNotFoundError",
                        "detail": "Run not found",
                    },
                ],
                "succeeded": 1,
                "failed": 1,
            },
        ),
    )
    def terminate_batch(
        payload: BatchTerminationRequest, adapter: Annotated[Any, Depends(get_adapter)]
    ):
        """逐项处理 1–200 个不同 Run。处理完成时返回 200，调用方必须检查 results 中的每一项。

        部分成功不会回滚，accepted 表示终止请求已接受，最终状态仍需查询 Run。
        上游通信失败而无法取得批次结果时返回 502；原 RPC 批量终止路径的 409 行为保持兼容。
        """
        return adapter.terminate_runs(payload.run_ids)

    return router
