import os
import glob
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import time
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import CollectorRegistry
from fastapi.responses import Response
import requests as Req

from dex_auth import get_istio_auth_session

custom_registry = CollectorRegistry()
app = FastAPI(title="XGBoost Inference API")

# Prometheus Metrics
REQUEST_COUNT = Counter(
    "inference_request_total", 
    "Total number of inference requests", 
    ["endpoint", "status"],
    registry=custom_registry  
)
LATENCY = Histogram(
    "inference_latency_seconds", 
    "Time spent processing inference requests",
    ["endpoint"],
    registry=custom_registry  
)
MODEL_ERRORS = Counter(
    "model_errors_total", 
    "Total number of model errors",
    ["error_type"],
    registry=custom_registry 
)
FEATURE_MISSING = Counter(
    "feature_missing_total",
    "Number of times features were missing in Redis",
    registry=custom_registry  
)

@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

@app.get("/metrics")
async def metrics():
    return Response(
        content=generate_latest(custom_registry),
        media_type="text/plain"
    )

# Configuration
MODEL_DIR = "/app/notebook/model-checkpoints/final-model/xgb_model"
REDIS_HOST = "redis"
REDIS_PORT = 6379
FEATURE_COLUMNS = [
    "brand", 
    "price", 
    "event_weekday", 
    "category_code_level1", 
    "category_code_level2", 
    "activity_count"
]

# Connect to Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Global model variables
loaded_model = None
current_model_file = None

class PredictionRequest(BaseModel):
    user_id: int = 571535080
    product_id: int = 12300394
    user_session: str = "a7d319fe-1894-4a87-8dad-0535466d9a57"

class PredictionResponse(BaseModel):
    predictions: List[float]
    success: bool = True
    error: Optional[str] = None
    predictor_status: Optional[str] = None


def get_features_from_redis(user_id: int, product_id: int, user_session: str) -> Dict[str, Any]:
    """Retrieve features from Redis."""
    try:
        key = f"user:{user_id}:product:{product_id}:session:{user_session}"
        key_type = redis_client.type(key)
        print(f"Key type for {key} is {key_type}")

        feature_data = redis_client.hgetall(key)
        
        if not feature_data:
            key = f"user:{user_id}:product:{product_id}:session:{user_session}"
            feature_data = redis_client.hgetall(key)
            
        if not feature_data:
            return {"success": False, "error": "Features not found in Redis"}

        feature_dict = {}
        for key, value in feature_data.items():
            raw_value = value[0] if isinstance(value, list) else value
            try:
                feature_dict[key] = float(raw_value)
            except (ValueError, TypeError):
                feature_dict[key] = 0.0

        feature_values = {
            key: feature_dict.get(key, 0.0)
            for key in FEATURE_COLUMNS
        }

        return {"success": True, "feature_values": [feature_values]}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/")
async def root():
    return {"message": "XGBoost Inference API is running"}

@app.post("/predict", response_model=PredictionResponse)
async def predict(requests: List[PredictionRequest]):
    """Run inference using the XGBoost model."""
    start_time = time.time()
    REQUEST_COUNT.labels(endpoint="/predict", status="started").inc()
    try:
        features = []
        
        for request in requests:
            feature_result = get_features_from_redis(
                user_id=request.user_id, 
                product_id=request.product_id, 
                user_session=request.user_session
            )
            print(f"Feature result: {feature_result}")
            
            if not feature_result["success"]:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error: {feature_result['error']} for user_id={request.user_id}, product_id={request.product_id}, session={request.user_session}",
                )
            features.append(feature_result["feature_values"])
        
        REQUEST_COUNT.labels(endpoint="/predict", status="success").inc()
        LATENCY.labels(endpoint="/predict").observe(time.time() - start_time)
        
        KUBEFLOW_ENDPOINT = "http://35.247.171.74:8080/"
        KUBEFLOW_USERNAME = "bigdata@gmail.com"
        KUBEFLOW_PASSWORD = "12341234"
        MODEL_NAME = "bigdata-xgb"
        SERVICE_HOSTNAME = "bigdata-xgb.bigdata.example.com"
        PREDICT_ENDPOINT = f"{KUBEFLOW_ENDPOINT}/v1/models/{MODEL_NAME}:predict"
        infer_input = {"instances": features}

        _auth_session = get_istio_auth_session(
            url=KUBEFLOW_ENDPOINT, 
            username=KUBEFLOW_USERNAME, 
            password=KUBEFLOW_PASSWORD
        )

        cookies = {"authservice_session": _auth_session["authservice_session"]}
        jar = Req.cookies.cookiejar_from_dict(cookies)

        response = Req.post(
            url=PREDICT_ENDPOINT,
            headers={"Host": SERVICE_HOSTNAME, "Content-Type": "application/json"},
            cookies=jar,
            json=infer_input,
            timeout=200,
        )
        
        status = response.status_code
        if status != 200:
            raise HTTPException(status_code=500, detail="Server Error")

        predictions = response.json().get("predictions")

        return PredictionResponse(
            predictions=predictions,
            success=True,
            predictor_status=str(status)
        )
        
    except Exception as e:
        REQUEST_COUNT.labels(endpoint="/predict", status="error").inc()
        MODEL_ERRORS.labels(error_type=type(e).__name__).inc()
        LATENCY.labels(endpoint="/predict").observe(time.time() - start_time)
        return PredictionResponse(
            predictions=[],
            success=False,
            error=str(e)
        )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
