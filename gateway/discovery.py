"""Read-only discovery, configuration validation and run diagnostics resources."""

import datetime
import json
from typing import Annotated, Any, Literal

from dagster import DagsterRunStatus
from fastapi import APIRouter, Depends, Path, Query
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    model_validator,
)

from gateway.dagster_adapter import DagsterAdapter, build_runs_filter, encode_job_id
from gateway.errors import GatewayHTTPError, responses

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
AssetPath = Annotated[list[NonEmpty], Field(min_length=1)]
EXAMPLE_JOB_ID = encode_job_id("my_code", "my_repository", "daily_job")
EXAMPLE_RUN_ID = "12345678-1234-1234-1234-123456789abc"
JobId = Annotated[
    str,
    Path(
        min_length=1,
        max_length=4096,
        description="发现接口返回的唯一 Job ID，编码代码位置、Repository、Job 名称。",
        examples=[EXAMPLE_JOB_ID],
    ),
]
RunId = Annotated[
    NonEmpty, Path(description="Dagster Run ID。", examples=[EXAMPLE_RUN_ID])
]


class LocationRecord(BaseModel):
    location_name: str = Field(description="Dagster Workspace 中的代码位置名称。")
    load_status: str = Field(
        description="Dagster 报告的加载状态。", examples=["LOADED"]
    )
    updated_at: datetime.datetime | None = Field(
        None, description="最近更新的 UTC 时间。"
    )
    loaded: bool = Field(description="位置当前是否加载成功。")
    reload_supported: bool = Field(description="位置是否支持重载。")
    repositories: list[str] = Field(description="加载成功的 Repository 名称列表。")
    error: str | None = Field(None, description="加载错误摘要；加载成功时为空。")


class LocationsResponse(BaseModel):
    items: list[LocationRecord] = Field(description="当前位置完整列表；此接口不分页。")


class JobRecord(BaseModel):
    job_id: str = Field(
        description="位置、Repository、Job 名称组成的 URL 安全唯一标识。使用此值请求详情和提交。",
        examples=[EXAMPLE_JOB_ID],
    )
    job_name: str = Field(
        description="Job 名称，可能在不同位置或 Repository 中重复。",
        examples=["daily_job"],
    )
    repository_location_name: str = Field(
        description="Job 所属代码位置。", examples=["my_code"]
    )
    repository_name: str = Field(
        description="Job 所属 Repository。", examples=["my_repository"]
    )
    description: str | None = Field(None, description="Job 定义中的说明。")
    is_asset_job: bool = Field(description="该 Job 是否为资产物化 Job。")
    tags: dict[str, str] = Field(description="Job 定义标签。")


class JobDetail(JobRecord):
    op_names: list[str] = Field(description="Job 包含的 op 节点名称。")


class JobsResponse(BaseModel):
    items: list[JobRecord] = Field(
        description="满足条件的完整 Job 列表；本接口不分页。"
    )


class ConfigField(BaseModel):
    name: str = Field(description="配置字段名称。")
    description: str | None = Field(None, description="字段说明。")
    configTypeKey: str = Field(description="在 types 中可查找的配置类型键。")
    isRequired: bool = Field(description="字段是否必填。")
    isSecret: bool = Field(description="Dagster 是否将该字段标记为 Secret。")
    defaultValueAsJson: str | None = Field(
        None, description="JSON 编码的默认值；没有默认值时为空。"
    )


class ConfigType(BaseModel):
    key: str = Field(description="Dagster 原生配置类型标识。")
    description: str | None = Field(None, description="配置类型说明。")
    typeParamKeys: list[str] = Field(description="类型参数引用的其他类型键。")
    isSelector: bool = Field(description="是否为只允许选取一个分支的 Selector。")
    fields: list[ConfigField] | None = Field(None, description="复合配置类型的字段。")
    model_config = ConfigDict(extra="allow")


class ConfigSchemaResponse(BaseModel):
    job_id: str = Field(description="被查询的 Job 唯一标识。")
    format: str = Field(
        description="配置格式固定为 dagster，不是 JSON Schema。", examples=["dagster"]
    )
    root_type_key: str = Field(description="根配置类型在 types 中的键。")
    default_yaml: str = Field(
        description="Dagster 根据配置定义生成的默认 YAML；不保证无需补充即可执行。"
    )
    types: list[ConfigType] = Field(
        description="Dagster 原生配置类型图；通过 key、configTypeKey 和 typeParamKeys 关联。包含原生 __typename 及相应类型的扩展字段。"
    )


class ValidationRequest(BaseModel):
    run_config: dict[str, Any] = Field(
        default_factory=dict, description="要预检的 Job 运行配置。", examples=[{}]
    )
    op_selection: list[NonEmpty] | None = Field(
        None, description="可选的 Dagster op 执行范围。", examples=[["extract"]]
    )
    asset_selection: list[NonEmpty | AssetPath] | None = Field(
        None,
        description="可选的资产键；多段键用字符串数组表达。",
        examples=[[["warehouse", "orders"]]],
    )
    model_config = ConfigDict(
        extra="forbid", json_schema_extra={"examples": [{"run_config": {}}]}
    )

    @model_validator(mode="after")
    def finite_json(self):
        try:
            json.dumps(self.model_dump(), allow_nan=False)
        except (ValueError, TypeError):
            raise ValueError("run_config must contain finite JSON values") from None
        return self


class ConfigValidationIssue(BaseModel):
    message: str = Field(description="目标 Job 配置验证错误说明。")
    path: list[str | int] = Field(
        description="相对于 run_config 的错误路径：字符串为字段名或 Map 键，整数为数组下标。根级错误为空数组；缺失或未知字段包含字段本身，多个字段分别返回错误项。Selector 选择错误定位到所属对象。",
        examples=[["ops", "extract", "config", "items", 0, "count"]],
    )
    reason: str = Field(description="Dagster 原生配置验证错误原因。")


class ValidationResponse(BaseModel):
    job_id: str = Field(description="被验证的 Job 唯一标识。")
    valid: bool = Field(description="运行配置是否符合目标 Job 和执行范围。")
    errors: list[ConfigValidationIssue] = Field(
        description="配置错误列表；有效时为空。"
    )


class RunRecord(BaseModel):
    run_id: str = Field(description="Dagster Run 唯一标识。", examples=[EXAMPLE_RUN_ID])
    job_name: str = Field(description="运行的 Job 名称。")
    job_id: str | None = Field(
        None,
        description="根据运行原始代码位置、Repository 和 Job 名称构成的 ID；历史运行缺少来源时为空。",
    )
    repository_location_name: str | None = Field(
        None, description="运行原始代码位置；缺少来源信息时为空。"
    )
    repository_name: str | None = Field(
        None, description="运行原始 Repository；缺少来源信息时为空。"
    )
    status: DagsterRunStatus = Field(description="Dagster 当前运行状态。")
    created_at: datetime.datetime | None = Field(
        None, description="Run 创建时间，UTC。"
    )
    started_at: datetime.datetime | None = Field(
        None, description="执行开始时间，UTC；未开始时为空。"
    )
    ended_at: datetime.datetime | None = Field(
        None, description="执行结束时间，UTC；未结束时为空。"
    )
    parent_run_id: str | None = Field(None, description="重执行所关联的父 Run。")
    root_run_id: str | None = Field(None, description="重执行链的根 Run。")
    tags: dict[str, str] = Field(description="运行标签。")
    failed_steps: list[str] = Field(
        description="Dagster 步骤统计中失败的步骤名；并非所有运行失败都对应步骤失败。"
    )
    failure_summary: str | None = Field(
        None, description="失败状态与步骤摘要；精确异常原因通过事件接口获取。"
    )


class RunsResponse(BaseModel):
    items: list[RunRecord] = Field(description="当前页运行，顺序由 Dagster 返回。")
    next_cursor: str | None = Field(
        None,
        description="还有下一页时，返回本页最后一个 Run ID；否则为空。与事件游标不能混用。",
    )
    has_more: bool = Field(description="查询时是否还有更多运行。")


class EventError(BaseModel):
    message: str = Field(description="Dagster 失败事件中的 Python 异常说明。")
    className: str | None = Field(None, description="Python 异常类名。")


class RunEvent(BaseModel):
    event_type: str = Field(
        description="Dagster 事件类型；没有 eventType 时使用 GraphQL 事件类型名。"
    )
    message: str = Field(description="事件消息。")
    timestamp: str | None = Field(
        None, description="Dagster 原生 Unix 毫秒时间戳字符串。"
    )
    level: str | None = Field(None, description="事件日志级别。")
    step_key: str | None = Field(None, description="事件所属步骤；运行级事件可能为空。")
    error: EventError | None = Field(
        None, description="失败事件携带的 Python 异常摘要。"
    )


class EventsResponse(BaseModel):
    run_id: str = Field(description="事件所属 Run。")
    items: list[RunEvent] = Field(
        description="当前页结构化事件；不是原始 stdout/stderr 文件。"
    )
    next_cursor: str = Field(
        description="Dagster 不透明的事件尾部游标；即使 has_more=false 也可用于稍后继续轮询。"
    )
    has_more: bool = Field(description="当前查询时刻是否还有未返回的事件。")


class OutputMetadataEntry(BaseModel):
    label: str = Field(description="Dagster 输出元数据的标签，例如 api_value、path。")
    type: str = Field(
        description="Dagster GraphQL 元数据类型名称，例如 JsonMetadataEntry、PathMetadataEntry。"
    )
    description: str | None = Field(None, description="元数据项说明。")
    value: JsonValue = Field(
        None,
        description="元数据的 JSON 预览；不支持展开的类型可为空。单项预览最多约 4 KiB，所有预览另共享 1 MiB 预算；不能将普通元数据自动视为 op 实际返回值。预算耗尽时可为空，截断时也可能为字符串预览。",
    )
    value_available: bool = Field(
        description="元数据值是否完整且类型已识别；true/value=null 表示明确的 null。未知类型或预览截断时为 false。此字段不表示 op 实际返回值可读。"
    )
    truncated: bool = Field(description="元数据预览是否因大小限制而被截断。")


class RunOutputEvent(BaseModel):
    event_type: Literal["STEP_OUTPUT", "HANDLED_OUTPUT"] = Field(
        description="STEP_OUTPUT 表示 op 产生输出及类型检查；HANDLED_OUTPUT 表示 IO Manager 已处理输出。两者独立返回，不代表实际值一定可读。"
    )
    output_name: str = Field(
        description="op 声明的输出名称，普通单输出通常为 result。", examples=["result"]
    )
    timestamp: datetime.datetime | None = Field(
        None,
        description="输出事件的 UTC 时间；本接口统一使用 ISO 8601，与旧 events 接口的原生毫秒字符串不同。",
    )
    type_check_success: bool | None = Field(
        None,
        description="STEP_OUTPUT 的类型检查结果；HANDLED_OUTPUT 没有该字段时为空。产生输出后步骤仍可能失败。",
    )
    manager_key: str | None = Field(
        None,
        description="HANDLED_OUTPUT 报告的 IO Manager 资源键；STEP_OUTPUT 通常为空。",
    )
    metadata: list[OutputMetadataEntry] = Field(
        description="该次输出事件的元数据预览；不自动合并其他事件。"
    )
    value_available: bool = Field(
        description="是否从明确约定的结果元数据中读取到实际值；true 且 value=null 表示实际值就是 null。"
    )
    value: JsonValue = Field(
        None,
        description="读取到的 JSON 实际值，可为对象、数组、字符串、数字、布尔或 null。仅在 value_available=true 时解释为已导出的实际值。",
    )
    value_unavailable_reason: (
        Literal["NOT_EXPOSED", "INVALID_VALUE", "VALUE_TOO_LARGE", "RESPONSE_LIMIT"]
        | None
    ) = Field(
        None,
        description="不可读取原因：NOT_EXPOSED 为该事件未按约定导出结果；INVALID_VALUE 为类型不支持、不是合法有限 JSON 或嵌套超过 64 层；VALUE_TOO_LARGE 为单值超过 64 KiB；RESPONSE_LIMIT 为本次响应实际值累计超过 1 MiB。可读取时为空。",
    )
    value_source: str | None = Field(
        None, description="用于读取结果的元数据标签，例如 api_value；没有对应项时为空。"
    )


class RunStepAttempt(BaseModel):
    started_at: datetime.datetime | None = Field(
        None, description="本次步骤执行尝试开始的 UTC 时间。"
    )
    ended_at: datetime.datetime | None = Field(
        None, description="本次尝试结束的 UTC 时间；尚未结束时为空。"
    )


class RunStepOutputs(BaseModel):
    step_key: str = Field(
        description="Run 内的实际执行步骤键。动态映射的各实例可能分别出现，例如 process[a]。",
        examples=["extract"],
    )
    op_handle: str | None = Field(
        None,
        description="步骤对应的 op 句柄，包含嵌套图路径；无法从历史计划或事件确定时为空。多个动态实例可共享同一个句柄。",
    )
    kind: Literal["COMPUTE", "UNRESOLVED_MAPPED", "UNRESOLVED_COLLECT"] | None = Field(
        None,
        description="Dagster 执行计划步骤类型；缺少计划信息时为空。未解析的动态步骤是计划模板，不等同于实际执行实例。",
    )
    status: Literal[
        "SKIPPED", "SUCCESS", "FAILURE", "IN_PROGRESS", "NOT_STARTED", "UNKNOWN"
    ] = Field(
        description="Dagster 步骤统计状态；计划中既无统计也无事件的步骤为 NOT_STARTED，已观察到输出却缺少统计或统计状态不确定时为 UNKNOWN。Run 取消不会被直接推断为每个步骤均取消。"
    )
    started_at: datetime.datetime | None = Field(
        None, description="步骤开始的 UTC 时间；未启动或无统计时为空。"
    )
    ended_at: datetime.datetime | None = Field(
        None, description="步骤结束的 UTC 时间；未结束或无统计时为空。"
    )
    attempts: list[RunStepAttempt] = Field(
        description="Dagster 记录的执行尝试，包括重试。输出事件按原始顺序保留，不直接归并或覆盖不同尝试。"
    )
    declared_outputs: list[str] = Field(
        description="执行计划中声明的输出名称；缺少历史计划时可能为空。没有输出事件不代表没有声明输出。"
    )
    outputs: list[RunOutputEvent] = Field(
        description="本次扫描范围内该步骤的全部输出事件。相同输出名可能因动态输出、重试及 IO Manager 处理而出现多次；未产生输出时为空。"
    )


class RunOutputsResponse(BaseModel):
    run_id: str = Field(description="被查询的 Run ID。", examples=[EXAMPLE_RUN_ID])
    run_status: DagsterRunStatus = Field(
        description="读取步骤快照时 Dagster 报告的 Run 状态；与后续事件查询不构成原子快照。"
    )
    execution_plan_available: bool = Field(
        description="是否取得运行的执行计划。false 时无法补齐既无步骤统计也无输出事件的未启动 op，items 仅包含可见条目。"
    )
    items: list[RunStepOutputs] = Field(
        description="执行计划、步骤统计和本次输出事件中可见的步骤。一个 op 的多个动态实例可分别返回；历史计划缺失时无法补齐没有统计或事件的步骤。"
    )
    scanned_events: int = Field(
        description="本次实际扫描的全部 Dagster 事件数量，包括没有输出的事件；不等于返回的输出条数。"
    )
    next_cursor: str | None = Field(
        None,
        description="本次扫描结束位置的 Dagster 事件游标；可继续增量读取，即使 has_more=false 也可稍后用于轮询。不要使用 Run 列表游标。",
    )
    has_more: bool = Field(
        description="查询时刻是否还有事件未扫描；达到 max_events 时可能为 true，需要携带 next_cursor 继续查询。"
    )
    complete: bool = Field(
        description="仅当本次未传 cursor 且扫描到当前事件尾部时为 true，表示本次响应包含从头至当前尾部的输出事件；不表示 Run 已结束，也不保证历史执行计划存在。"
    )


OUTPUTS_EXAMPLE = {
    "run_id": EXAMPLE_RUN_ID,
    "run_status": "SUCCESS",
    "execution_plan_available": True,
    "items": [
        {
            "step_key": "extract",
            "op_handle": "extract",
            "kind": "COMPUTE",
            "status": "SUCCESS",
            "started_at": "2026-11-08T00:00:01Z",
            "ended_at": "2026-11-08T00:00:02Z",
            "attempts": [
                {
                    "started_at": "2026-11-08T00:00:01Z",
                    "ended_at": "2026-11-08T00:00:02Z",
                }
            ],
            "declared_outputs": ["result"],
            "outputs": [
                {
                    "event_type": "STEP_OUTPUT",
                    "output_name": "result",
                    "timestamp": "2026-11-08T00:00:02Z",
                    "type_check_success": True,
                    "manager_key": None,
                    "metadata": [
                        {
                            "label": "api_value",
                            "type": "JsonMetadataEntry",
                            "description": None,
                            "value": {"rows": 12},
                            "value_available": True,
                            "truncated": False,
                        }
                    ],
                    "value_available": True,
                    "value": {"rows": 12},
                    "value_unavailable_reason": None,
                    "value_source": "api_value",
                },
                {
                    "event_type": "HANDLED_OUTPUT",
                    "output_name": "result",
                    "timestamp": "2026-11-08T00:00:02Z",
                    "type_check_success": None,
                    "manager_key": "io_manager",
                    "metadata": [
                        {
                            "label": "path",
                            "type": "PathMetadataEntry",
                            "description": None,
                            "value": "/storage/run/extract/result",
                            "value_available": True,
                            "truncated": False,
                        }
                    ],
                    "value_available": False,
                    "value": None,
                    "value_unavailable_reason": "NOT_EXPOSED",
                    "value_source": None,
                },
            ],
        }
    ],
    "scanned_events": 10,
    "next_cursor": "opaque-dagster-event-cursor",
    "has_more": False,
    "complete": True,
}


JOB_EXAMPLE = {
    "job_id": EXAMPLE_JOB_ID,
    "job_name": "daily_job",
    "repository_location_name": "my_code",
    "repository_name": "my_repository",
    "description": "每日数据任务",
    "is_asset_job": False,
    "tags": {},
}
RUN_EXAMPLE = {
    "run_id": EXAMPLE_RUN_ID,
    "job_name": "daily_job",
    "status": "SUCCESS",
    "job_id": EXAMPLE_JOB_ID,
    "repository_location_name": "my_code",
    "repository_name": "my_repository",
    "created_at": "2026-11-08T00:00:00Z",
    "started_at": "2026-11-08T00:00:01Z",
    "ended_at": "2026-11-08T00:00:05Z",
    "parent_run_id": None,
    "root_run_id": None,
    "tags": {},
    "failed_steps": [],
    "failure_summary": None,
}


def create_discovery_router(get_adapter, verify_token) -> APIRouter:
    router = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_token)])
    adapter_dependency = Depends(get_adapter)

    @router.get(
        "/locations",
        response_model=LocationsResponse,
        tags=["Locations"],
        summary="发现代码位置",
        operation_id="list_locations",
        responses=responses(
            "代码位置完整列表。",
            {
                "items": [
                    {
                        "location_name": "my_code",
                        "load_status": "LOADED",
                        "updated_at": "2026-11-08T00:00:00Z",
                        "loaded": True,
                        "reload_supported": True,
                        "repositories": ["my_repository"],
                        "error": None,
                    }
                ]
            },
        ),
    )
    def locations(adapter: DagsterAdapter = adapter_dependency):
        """列出 Workspace 中全部代码位置，包括加载失败的位置及其错误摘要。当前接口不分页。"""
        return adapter.list_locations()

    @router.get(
        "/jobs",
        response_model=JobsResponse,
        tags=["Jobs"],
        summary="发现可运行 Job",
        operation_id="list_jobs",
        responses=responses("符合筛选条件的完整 Job 列表。", {"items": [JOB_EXAMPLE]}),
    )
    def jobs(
        location_name: Annotated[
            NonEmpty | None,
            Query(description="可选代码位置名称；精确匹配。", examples=["my_code"]),
        ] = None,
        repository_name: Annotated[
            NonEmpty | None,
            Query(
                description="可选 Repository 名称；精确匹配。",
                examples=["my_repository"],
            ),
        ] = None,
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """获取完整 Job 发现列表。当前接口不分页。

        job_id 编码了代码位置、Repository 和 Job 名称，因此不同位置的同名 Job 具有不同 ID。
        请保留返回的 job_id 用于后续配置查询、预检和提交，不要用 job_name 替代。
        """
        return adapter.list_jobs(location_name, repository_name)

    @router.get(
        "/jobs/{job_id}",
        response_model=JobDetail,
        tags=["Jobs"],
        summary="获取 Job 详情",
        operation_id="get_job",
        responses=responses(
            "Job 定义与所属位置。", {**JOB_EXAMPLE, "op_names": ["extract", "load"]}
        ),
    )
    def job(job_id: JobId, adapter: DagsterAdapter = adapter_dependency):
        """使用发现接口返回的 job_id 查询唯一 Job，包含 op 名称。ID 无效返回 422，Job 不存在返回 404。"""
        return adapter.get_job(job_id)

    @router.get(
        "/jobs/{job_id}/config-schema",
        response_model=ConfigSchemaResponse,
        tags=["Jobs"],
        summary="获取 Job 配置类型图",
        operation_id="get_job_config_schema",
        responses=responses(
            "Dagster 原生配置类型图。",
            {
                "job_id": EXAMPLE_JOB_ID,
                "format": "dagster",
                "root_type_key": "Shape.example",
                "default_yaml": "{}\n",
                "types": [
                    {
                        "__typename": "CompositeConfigType",
                        "key": "Shape.example",
                        "description": None,
                        "typeParamKeys": [],
                        "isSelector": False,
                        "fields": [],
                    }
                ],
            },
        ),
    )
    def config_schema(job_id: JobId, adapter: DagsterAdapter = adapter_dependency):
        """返回目标 Job 的 Dagster 原生配置类型图和默认 YAML，不是标准 JSON Schema。

        从 root_type_key 开始，通过 configTypeKey/typeParamKeys 引用查找 types 中的类型。
        对复合类型读取 fields；枚举、数组、Map 等保留 Dagster 的原生扩展字段。
        默认 YAML 是配置提示，提交前仍应调用预检接口。
        """
        return adapter.get_config_schema(job_id)

    @router.post(
        "/jobs/{job_id}/validate",
        response_model=ValidationResponse,
        tags=["Jobs"],
        summary="预检 Job 配置",
        operation_id="validate_job_config",
        responses=responses(
            "配置验证已完成；valid=false 表示配置无效，不创建 Run。",
            {
                "job_id": EXAMPLE_JOB_ID,
                "valid": False,
                "errors": [
                    {
                        "message": "Missing required config entry",
                        "path": ["ops", "extract", "config"],
                        "reason": "MISSING_REQUIRED_FIELD",
                    }
                ],
            },
        ),
    )
    def validate(
        job_id: JobId,
        payload: ValidationRequest,
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """校验运行配置与 op/asset 执行选择，不创建 Run、不启动执行。

        完成验证返回 200，具体是否通过由 valid 表示；请求字段拼写错误返回 422。
        errors.path 相对于 run_config，包含字段名、数组下标或 Map 键；缺失/未知字段会定位到字段本身。
        一条上游错误涉及多个缺失或未知字段时，按字段分别返回错误项；根级或 Selector 错误可定位到整个对象。
        验证通过只代表配置符合当前 Job 定义，后续执行仍可能因资源或运行环境失败。
        """
        return adapter.validate_job(job_id, **payload.model_dump())

    @router.get(
        "/runs",
        response_model=RunsResponse,
        tags=["Runs"],
        summary="分页查询运行",
        operation_id="list_runs",
        responses=responses(
            "当前页运行。",
            {"items": [RUN_EXAMPLE], "next_cursor": None, "has_more": False},
        ),
    )
    def runs(
        statuses: Annotated[
            list[DagsterRunStatus] | None,
            Query(
                description="可重复指定状态，例如 ?statuses=SUCCESS&statuses=FAILURE。使用 OR 关系。"
            ),
        ] = None,
        job_name: Annotated[
            NonEmpty | None,
            Query(description="可选 Job 名称；同名 Job 的不同代码位置会一起匹配。"),
        ] = None,
        job_id: Annotated[
            NonEmpty | None,
            Query(
                max_length=4096,
                description="发现接口返回的复合 Job ID；精确筛选代码位置、Repository 和 Job。与 job_name 或保留 Repository 标签冲突时返回 422。",
                examples=[EXAMPLE_JOB_ID],
            ),
        ] = None,
        tag: Annotated[
            list[str] | None,
            Query(
                description="可重复指定 key=value 标签，使用 AND 关系；值可包含等号。",
                examples=[["source=webhook"]],
            ),
        ] = None,
        cursor: Annotated[
            NonEmpty | None,
            Query(description="上一页返回的 next_cursor（Run ID）；不能使用事件游标。"),
        ] = None,
        limit: Annotated[
            int, Query(ge=1, le=200, description="每页最多返回的运行数量。")
        ] = 50,
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """按状态、唯一 Job ID 或 Job 名称、标签分页查询运行，不返回完整运行配置。

        使用返回的 next_cursor 获取下一页。分页反映每次查询的实时数据，不提供跨页快照。
        重复相同标签键但值不同属于无效请求，不会静默覆盖。
        job_id 使用与 Dagster 原生 Job 运行列表相同的 Repository 标签筛选；
        同名 Job 可按 job_id 精确区分。缺少 Repository 来源标签的旧运行不会匹配 job_id，应改用 job_name 查询。
        """
        tags = {}
        for expression in tag or []:
            key, separator, value = expression.partition("=")
            if not separator or not key.strip():
                raise GatewayHTTPError(
                    422,
                    "REQUEST_VALIDATION_FAILED",
                    "tag must use a non-empty key=value form",
                )
            if key in tags and tags[key] != value:
                raise GatewayHTTPError(
                    422,
                    "REQUEST_VALIDATION_FAILED",
                    "Repeated tag keys must use the same value",
                )
            tags[key] = value
        build_runs_filter(job_name=job_name, tags=tags, job_id=job_id)
        return adapter.list_runs(
            statuses=[value.value for value in statuses] if statuses else None,
            job_name=job_name,
            tags=tags or None,
            cursor=cursor,
            limit=limit,
            job_id=job_id,
        )

    @router.get(
        "/runs/{run_id}",
        response_model=RunRecord,
        tags=["Runs"],
        summary="获取运行详情",
        operation_id="get_run",
        responses=responses("运行状态、时间和失败步骤摘要。", RUN_EXAMPLE),
    )
    def run(run_id: RunId, adapter: DagsterAdapter = adapter_dependency):
        """返回运行状态、UTC 时间、标签、父子运行关系和失败步骤摘要，不返回完整配置。

        failure_summary 是运行状态/步骤级摘要，精确 Python 异常与事件详情使用 events 接口查询。
        """
        return adapter.get_run(run_id)

    output_responses = responses(
        "步骤状态、逐次输出事件及可读取的实际值。", OUTPUTS_EXAMPLE
    )
    output_responses[200]["content"] = {
        "application/json": {
            "examples": {
                "readable_value": {
                    "summary": "成功步骤的 JSON 结果和 IO Manager 事件",
                    "value": OUTPUTS_EXAMPLE,
                },
                "not_started": {
                    "summary": "计划中的 op 尚未开始，没有输出事件",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "run_status": "QUEUED",
                        "items": [
                            {
                                **OUTPUTS_EXAMPLE["items"][0],
                                "status": "NOT_STARTED",
                                "started_at": None,
                                "ended_at": None,
                                "attempts": [],
                                "outputs": [],
                            }
                        ],
                        "scanned_events": 0,
                    },
                },
                "failed_after_output": {
                    "summary": "op 产生输出后仍可失败；没有导出结果的事件不可读",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "run_status": "FAILURE",
                        "items": [
                            {
                                **OUTPUTS_EXAMPLE["items"][0],
                                "status": "FAILURE",
                                "outputs": [OUTPUTS_EXAMPLE["items"][0]["outputs"][1]],
                            }
                        ],
                    },
                },
                "partial": {
                    "summary": "达到扫描上限，需继续读取游标",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "scanned_events": 10000,
                        "has_more": True,
                        "complete": False,
                    },
                },
            }
        }
    }

    @router.get(
        "/runs/{run_id}/outputs",
        response_model=RunOutputsResponse,
        tags=["Runs"],
        summary="查询各 op 的输出和状态",
        operation_id="get_run_outputs",
        responses=output_responses,
    )
    def outputs(
        run_id: RunId,
        cursor: Annotated[
            NonEmpty | None,
            Query(
                description="上次响应的 next_cursor；不传时从事件开头扫描。传入游标时 outputs 只包含本段事件，步骤状态仍返回当前统计快照。",
                examples=["opaque-dagster-event-cursor"],
            ),
        ] = None,
        max_events: Annotated[
            int,
            Query(
                ge=1,
                le=20000,
                description="本次最多扫描的全部事件数，默认 10000；包括普通日志和非输出事件。达到上限后通过 has_more 和 next_cursor 继续。",
                examples=[10000],
            ),
        ] = 10000,
        value_metadata_key: Annotated[
            NonEmpty,
            Query(
                max_length=200,
                description="业务明确导出的实际结果元数据标签，默认 api_value。不会自动把其他元数据、路径或日志当作实际值。",
                examples=["api_value"],
            ),
        ] = "api_value",
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """读取 Run 的每个可见 op 执行步骤及输出，需要 read 权限，仅读取 Dagster。

        支持未开始、执行中、成功、失败、跳过以及重试。既没有步骤统计也没有事件的计划步骤标为 NOT_STARTED；
        已有输出却缺统计、或统计状态无法确定时标为 UNKNOWN。
        Run 为 CANCELED 时不推断所有步骤的状态。一个 op 的动态执行实例会分别返回，未解析模板通过 kind 区分。
        历史执行计划缺失时，仅能列出步骤统计和事件中可见的条目。

        outputs 逐个保留 STEP_OUTPUT 与 HANDLED_OUTPUT，不按输出名覆盖。前者表示输出和类型检查，
        后者表示 IO Manager 已处理；都不保证实际值可读。Dagster 1.13.26 GraphQL 不暴露动态输出的 mapping_key，
        因而同名动态输出保留为独立事件，但本接口不能可靠标识每个动态输出项的 mapping_key。

        实际值仅从 value_metadata_key 指定的明确导出元数据读取，支持 JSON、Text、Int、Float、Bool 和 Null。
        业务 op 可调用 context.add_output_metadata({"api_value": MetadataValue.json(result)}) 导出 JSON 结果。
        value_available=true 且 value=null 表示实际结果就是 null；false 时必须检查 value_unavailable_reason。
        普通 Out/return 由 IO Manager 保存，不会自动成为 API 可读值；文件路径、URL、pickle 或任意 Python 对象不直接读取。
        实际 JSON 值最多嵌套 64 层，过深返回 INVALID_VALUE。单值上限 64 KiB，本次实际值累计上限 1 MiB；
        元数据预览单项最多约 4 KiB，所有预览另共享 1 MiB。
        元数据截断由 truncated=true/value_available=false 标识；以上分别为值和预览预算，不是整份 HTTP 响应大小限制。
        RESPONSE_LIMIT 的输出事件仍会推进扫描游标；若要读取被预算省略的值，应从较早的游标减小 max_events 重读。

        max_events 限制全部扫描事件数。has_more=true 时携带 next_cursor 继续，不能把局部结果当完整输出。
        complete=true 仅表示本次从头扫描到了当前事件尾，不表示 Run 已完成；带 cursor 的增量响应 complete=false。
        complete=true 也不保证全部实际值都可读取，仍须逐项检查 value_available。
        游标到尾后仍可继续轮询。步骤统计与事件分开读取，运行中查询不是原子快照。
        本接口时间均为 UTC ISO 8601；旧 events 接口仍返回 Dagster 原生毫秒时间戳。
        """
        return adapter.get_run_outputs(
            run_id,
            cursor=cursor,
            max_events=max_events,
            value_metadata_key=value_metadata_key,
        )

    @router.get(
        "/runs/{run_id}/events",
        response_model=EventsResponse,
        tags=["Runs"],
        summary="增量查询运行事件",
        operation_id="get_run_events",
        responses=responses(
            "当前页结构化事件及继续轮询的游标。",
            {
                "run_id": EXAMPLE_RUN_ID,
                "items": [
                    {
                        "event_type": "RUN_SUCCESS",
                        "message": "Run completed",
                        "timestamp": "1794096000000",
                        "level": "DEBUG",
                        "step_key": None,
                        "error": None,
                    }
                ],
                "next_cursor": "opaque-dagster-event-cursor",
                "has_more": False,
            },
        ),
    )
    def events(
        run_id: RunId,
        cursor: Annotated[
            NonEmpty | None,
            Query(description="上次事件响应的 next_cursor；不要传入运行列表的游标。"),
        ] = None,
        limit: Annotated[
            int, Query(ge=1, le=1000, description="每页最多返回的事件数量。")
        ] = 100,
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """增量查询结构化事件，包含失败事件的异常摘要；不读取原始 stdout/stderr 文件。

        has_more=true 时继续翻页；false 表示当前已读到尾部，稍后仍可用 next_cursor 继续轮询。
        事件时间戳沿用 Dagster 原生 Unix 毫秒字符串，列表与事件游标不可互换。
        """
        return adapter.get_events(run_id, cursor, limit)

    return router
