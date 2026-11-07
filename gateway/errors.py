"""Documented gateway errors shared by the resource APIs."""

from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    detail: str | list[dict[str, Any]] = Field(
        description="错误说明或请求字段验证结果。"
    )
    code: str = Field(description="稳定的网关错误码。", examples=["RESOURCE_NOT_FOUND"])
    request_id: str = Field(description="与 X-Request-ID 响应头一致的请求标识。")
    retryable: bool = Field(
        description="能否按接口约定重试；提交重试必须使用相同幂等键。"
    )
    details: list[dict[str, Any]] = Field(
        default_factory=list, description="结构化错误信息。"
    )
    message: str | None = Field(None, description="上游错误来源。")
    error_type: str | None = Field(None, description="Dagster 错误类型（如适用）。")


class GatewayHTTPError(HTTPException):
    def __init__(
        self,
        status_code: int,
        code: str,
        detail: str,
        *,
        retryable: bool = False,
        details: list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(status_code, detail, headers)
        self.code = code
        self.retryable = retryable
        self.details = details or []


ERROR_RESPONSES = {
    code: {
        "model": ErrorResponse,
        "description": description,
        "content": {
            "application/json": {
                "example": {
                    "detail": description,
                    "code": error_code,
                    "request_id": "example-request",
                    "retryable": False,
                    "details": [],
                }
            }
        },
    }
    for code, error_code, description in (
        (401, "UNAUTHENTICATED", "缺少或无效的网关 Bearer Token。"),
        (403, "FORBIDDEN", "调用方没有此操作的权限。"),
        (404, "RESOURCE_NOT_FOUND", "请求的 Job、Run 或代码位置不存在。"),
        (409, "OPERATION_CONFLICT", "请求与现有操作或幂等记录冲突。"),
        (422, "REQUEST_VALIDATION_FAILED", "请求参数或目标 Job 的配置无效。"),
        (500, "INTERNAL_ERROR", "网关发生未预期的内部错误。"),
        (502, "DAGSTER_UNAVAILABLE", "Dagster 上游请求失败。"),
        (503, "NOT_READY", "Dagster 暂不可服务或本地持久化存储不可用。"),
    )
}


def responses(
    description: str, example: dict[str, Any], status_code: int = 200
) -> dict:
    return {
        **ERROR_RESPONSES,
        status_code: {
            "description": description,
            "content": {"application/json": {"example": example}},
        },
    }
