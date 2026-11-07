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


class RunOutputValue(BaseModel):
    output_name: str = Field(
        description="op 的输出名称，普通单输出通常为 result。", examples=["result"]
    )
    output_value: JsonValue = Field(
        description="从挂载存储文件中读出的 JSON 兼容结果。datetime、date、time 对象转换为 ISO 8601 字符串，可嵌套在列表或字典中；保留已有 UTC 偏移，无时区值不补时区。没有输出、没有可用路径、文件不存在、无法读取或无法转换时均为 null；实际结果为 null 时也返回 null。",
    )


class RunStepOutputs(BaseModel):
    step_key: str = Field(
        description="Run 内的执行步骤键；动态映射实例可分别出现，例如 process[a]。",
        examples=["extract"],
    )
    status: Literal[
        "SKIPPED", "SUCCESS", "FAILURE", "IN_PROGRESS", "NOT_STARTED", "UNKNOWN"
    ] = Field(
        description="Dagster 步骤状态。未观察到执行的计划步骤为 NOT_STARTED；状态快照无法确定时为 UNKNOWN。Run 取消不代表每个步骤均已取消。"
    )
    outputs: list[RunOutputValue] = Field(
        description="该步骤的输出名称及实际值；每项只有 output_name、output_value。本次扫描内同名同路径取最后一次；动态同名的不同路径分别保留。全量扫描时，尚无处理记录的声明输出补为 null；局部或增量扫描只返回本段观测到的输出。"
    )


class RunOutputsResponse(BaseModel):
    run_id: str = Field(description="被查询的 Run ID。", examples=[EXAMPLE_RUN_ID])
    run_status: DagsterRunStatus = Field(description="Dagster 报告的 Run 状态。")
    items: list[RunStepOutputs] = Field(
        description="各 op 执行步骤的状态和输出。历史计划缺失时，只能返回步骤统计和事件中可见的步骤。"
    )
    next_cursor: str | None = Field(
        None,
        description="继续读取事件的 Dagster 游标；即使 has_more=false 也可稍后继续轮询。",
    )
    has_more: bool = Field(
        description="查询时刻是否还有事件未扫描；为 true 时携带 next_cursor 继续。"
    )
    complete: bool = Field(
        description="仅当本次从事件开头扫描到当前尾部时为 true；不表示 Run 已结束，也不表示每个输出都可读取。"
    )


OUTPUTS_EXAMPLE = {
    "run_id": EXAMPLE_RUN_ID,
    "run_status": "SUCCESS",
    "items": [
        {
            "step_key": "gen_markdown",
            "status": "SUCCESS",
            "outputs": [
                {"output_name": "title", "output_value": "今日通知"},
                {"output_name": "markdown", "output_value": "## 今日通知\n处理完成。"},
            ],
        }
    ],
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
        "各步骤状态、输出名称及文件中的实际结果。", OUTPUTS_EXAMPLE
    )
    output_responses[200]["content"] = {
        "application/json": {
            "examples": {
                "readable_value": {
                    "summary": "从挂载的文件读取输出",
                    "value": OUTPUTS_EXAMPLE,
                },
                "not_started": {
                    "summary": "尚未开始，输出值为空",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "run_status": "QUEUED",
                        "items": [
                            {
                                "step_key": "gen_markdown",
                                "status": "NOT_STARTED",
                                "outputs": [
                                    {"output_name": "title", "output_value": None},
                                    {"output_name": "markdown", "output_value": None},
                                ],
                            }
                        ],
                    },
                },
                "unreadable_value": {
                    "summary": "没有可读取的文件时返回 null",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "items": [
                            {
                                "step_key": "extract",
                                "status": "SUCCESS",
                                "outputs": [
                                    {"output_name": "result", "output_value": None}
                                ],
                            }
                        ],
                    },
                },
                "datetime_value": {
                    "summary": "嵌套的日期时间转为 ISO 8601 字符串",
                    "value": {
                        **OUTPUTS_EXAMPLE,
                        "items": [
                            {
                                "step_key": "prepare_summary",
                                "status": "SUCCESS",
                                "outputs": [
                                    {
                                        "output_name": "result",
                                        "output_value": {
                                            "generated_at": "2026-11-08T09:15:00+08:00",
                                            "date": "2026-11-08",
                                            "time": "09:15:00",
                                            "items": [
                                                {"created_at": "2026-11-08T09:00:00"}
                                            ],
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                },
                "partial": {
                    "summary": "还有未扫描的事件，需继续读取游标",
                    "value": {**OUTPUTS_EXAMPLE, "has_more": True, "complete": False},
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
                description="上次响应的 next_cursor；省略时从事件开头扫描。传入游标时只处理本段输出，步骤状态仍为当前快照。",
                examples=["opaque-dagster-event-cursor"],
            ),
        ] = None,
        max_events: Annotated[
            int,
            Query(
                ge=1,
                le=20000,
                description="本次最多扫描的全部事件数，默认 10000；包括非输出事件。达到上限后用 next_cursor 继续。",
                examples=[10000],
            ),
        ] = 10000,
        adapter: DagsterAdapter = adapter_dependency,
    ):
        """返回各 op 执行步骤的状态，以及只有 output_name、output_value 的输出列表，需要 read 权限。

        仅读取已有 Run 和网关容器挂载的输出文件。使用 HANDLED_OUTPUT 中 manager_key=io_manager、
        label=path 的路径读取结果；没有路径、文件不存在、不可读取或结果不支持时，output_value=null。
        本次从头扫描至尾时，尚无处理记录的声明输出补为 null；局部或增量扫描不补占位输出。
        实际结果为 null 时也返回 null。

        不可读取时的输出项：
        ```json
        {"output_name": "result", "output_value": null}
        ```

        本次扫描内，相同输出名称和路径的重复记录取最后一次；同名动态输出的不同路径分别返回。
        STEP_OUTPUT 在 IO Manager 保存结果前记录输出及类型检查；HANDLED_OUTPUT 在其处理后记录。
        此接口使用后者读取文件；内存或自定义 IO Manager 不一定提供文件路径。
        网关默认读取 /opt/dagster/dagster_home/storage 目录，可通过 RUN_OUTPUT_STORAGE_ROOT 设置读取根目录。
        Compose 中应挂载真实存储到相同容器路径，例如 ./storage/io-storage:/opt/dagster/dagster_home/storage:ro。
        支持 JSON 文件及保存 JSON 基础类型的 pickle，元组转为数组。
        datetime、date、time 转为 ISO 8601 字符串，可嵌套在列表或字典中；已有 UTC 偏移保留，无时区值不补时区。
        日期时间支持标准库固定偏移和 zoneinfo 时区，兼容 Dagster 默认使用的 pickle protocol 4；第三方时区不在支持范围内。
        其他不支持的对象、缺失文件或未挂载的文件仍返回 null。
        单文件和单值上限均为 16 MiB，实际值最多 64 层，本次结果累计最多 32 MiB；超限输出返回 null。

        has_more=true 时携带 next_cursor 继续；complete=true 仅表示本次从头扫描到了当前事件尾部。
        运行中查询不是原子快照；步骤取消、失败、跳过或未启动的状态仍以 Dagster 统计为准。
        """
        return adapter.get_run_outputs(run_id, cursor=cursor, max_events=max_events)

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
