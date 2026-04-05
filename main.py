import datetime
import logging
import secrets
from typing import Any, Dict, List, Optional, Union

import uvicorn
from fastapi import FastAPI, Request, Query, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from dagster_graphql import DagsterGraphQLClientError, DagsterGraphQLClient

# ==========================================
# 1. 基础配置与日志
# ==========================================
import os

VERSION = "1.0.0"

API_TOKEN = os.getenv("APP_API_TOKEN", secrets.token_urlsafe(32))
if not os.getenv("APP_API_TOKEN"):
    logger = logging.getLogger('dagster_api_gateway')
    logger.warning(f"!!! No APP_API_TOKEN set. Using ephemeral token: {API_TOKEN}")

DAGSTER_HOST = os.getenv("DAGSTER_HOST", "webserver")
DAGSTER_PORT = int(os.getenv("DAGSTER_PORT", 3000))
DAGSTER_TIMEOUT = int(os.getenv("DAGSTER_TIMEOUT", 120))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", 8080))

logger = logging.getLogger('dagster_api_gateway')
logger.propagate = False
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s:\t\t%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

dagster_client = DagsterGraphQLClient(DAGSTER_HOST, port_number=DAGSTER_PORT, timeout=DAGSTER_TIMEOUT)

app = FastAPI(title="Dagster RPC/Webhook Gateway", version="1.0.0")

# ==========================================
# 2. Pydantic 数据模型 (用于 POST JSON 负载)
# ==========================================
class SubmitJobRequest(BaseModel):
    job_name: str = Field(..., description="要执行的 Dagster Job 名称", min_length=1, max_length=100)
    repository_location_name: Optional[str] = Field(None, description="Repository Location 名称")
    repository_name: Optional[str] = None
    run_config: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None
    op_selection: Optional[List[str]] = None
    asset_selection: Optional[List[Union[str, List[str]]]] = None


class RunActionRequest(BaseModel):
    run_id: str


class TerminateRunsRequest(BaseModel):
    run_ids: List[str] = Field(..., min_length=1, max_length=200, description="Batch terminate, max 100 runs")


class LocationActionRequest(BaseModel):
    location_name: str

# 安全验证组件
security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """验证 Bearer Token 的合法性"""
    if not secrets.compare_digest(credentials.credentials, API_TOKEN):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials

# ==========================================
# 3. 全局异常处理
# ==========================================
@app.exception_handler(DagsterGraphQLClientError)
def dagster_exception_handler(request: Request, exc: DagsterGraphQLClientError):
    logger.error(f"Dagster GraphQL Error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "Dagster Client Error", "detail": str(exc)},
    )


# ==========================================
# 4. RPC 风格 API 路由 (仅 GET + POST)
# ==========================================

@app.get("/health", summary="健康检查", tags=["Monitoring"])
async def health_check():
    """基础健康检查 - 服务是否运行"""
    return {
        "status": "healthy",
        "service": "dagster-webhook-gateway",
        "timestamp": datetime.datetime.now().isoformat(),
        "version": VERSION
    }

@app.post("/api/v1/submit_job", summary="提交 Job 执行", dependencies=[Depends(verify_token)])
def submit_job(payload: SubmitJobRequest):
    """通过 JSON Body 传递 job_name 及其他参数"""
    run_id = dagster_client.submit_job_execution(
        job_name=payload.job_name,
        repository_location_name=payload.repository_location_name,
        repository_name=payload.repository_name,
        run_config=payload.run_config,
        tags=payload.tags,
        op_selection=payload.op_selection,
        asset_selection=payload.asset_selection
    )
    logger.info(f"Submitted Job [{payload.job_name}] -> Run ID: {run_id}")
    return {"message": "success", "run_id": run_id}


@app.get("/api/v1/get_run_status", summary="获取 Run 状态", dependencies=[Depends(verify_token)])
def get_run_status(run_id: str = Query(..., description="Run ID of the pipeline run")):
    """通过 URL Query 参数传递 run_id: /api/v1/get_run_status?run_id=xxx"""
    run_status = dagster_client.get_run_status(run_id)
    return {"run_id": run_id, "status": run_status.value}


@app.post("/api/v1/terminate_run", summary="终止单个 Run", dependencies=[Depends(verify_token)])
def terminate_run(payload: RunActionRequest):
    """通过 JSON Body 传递 run_id"""
    dagster_client.terminate_run(payload.run_id)
    logger.info(f"Terminated Run ID: {payload.run_id}")
    return {"message": "terminate signal sent", "run_id": payload.run_id}


@app.post("/api/v1/terminate_runs", summary="批量终止 Runs", dependencies=[Depends(verify_token)])
def terminate_runs(payload: TerminateRunsRequest):
    """通过 JSON Body 传递 run_ids 列表"""
    dagster_client.terminate_runs(payload.run_ids)
    logger.info(f"Terminated Batch Run IDs: {payload.run_ids}")
    return {"message": "batch terminate signal sent", "run_ids": payload.run_ids}


@app.post("/api/v1/reload_location", summary="重载 Repository Location", dependencies=[Depends(verify_token)])
def reload_location(payload: LocationActionRequest):
    """通过 JSON Body 传递 location_name"""
    result = dagster_client.reload_repository_location(payload.location_name)
    logger.info(f"Reloaded location: {payload.location_name}")
    return {"message": "reloaded", "detail": str(result)}


@app.post("/api/v1/shutdown_location", summary="关闭 Repository Location (Deprecated)", dependencies=[Depends(verify_token)])
def shutdown_location(payload: LocationActionRequest):
    """通过 JSON Body 传递 location_name"""
    result = dagster_client.shutdown_repository_location(payload.location_name)
    logger.info(f"Shut down location: {payload.location_name}")
    return {"message": "shutdown", "detail": str(result)}


if __name__ == "__main__":
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)