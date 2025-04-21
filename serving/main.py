import os
import glob
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import redis
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

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

app = FastAPI(title="XGBoost Inference API")

# Connect to Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Global model variable
loaded_model = None
current_model_file = None

class PredictionRequest(BaseModel):
    user_id: int = 530834332
    product_id: int = 1005073

class PredictionResponse(BaseModel):
    predictions: List[float]
    success: bool = True
    model_file: Optional[str] = None
    error: Optional[str] = None

def find_latest_model() -> str:
    """Find the most recent model checkpoint in the specified directory."""
    pattern = os.path.join(MODEL_DIR, "xgboost_model_*.ubj")
    model_files = glob.glob(pattern)
    
    if not model_files:
        raise FileNotFoundError(f"No model files found in {MODEL_DIR}")
    
    # Extract date from filename and find the most recent one
    latest_model = None
    latest_date = None
    
    for model_file in model_files:
        # Extract date from filename (format: xgboost_model_DD_MM_YYYY.ubj)
        match = re.search(r'xgboost_model_(\d{2})_(\d{2})_(\d{4})\.ubj', model_file)
        if match:
            day, month, year = map(int, match.groups())
            file_date = datetime(year, month, day)
            
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_model = model_file
    
    if latest_model is None:
        raise FileNotFoundError("Could not parse dates from model filenames")
        
    return latest_model

def load_model():
    """Load the latest XGBoost model."""
    global loaded_model, current_model_file
    try:
        model_file = find_latest_model()
        
        # Only reload if the model file has changed
        if current_model_file != model_file:
            print(f"Loading model from {model_file}")
            model = xgb.Booster()
            model.load_model(model_file)
            loaded_model = model
            current_model_file = model_file
        
        return loaded_model, current_model_file
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise

def get_features_from_redis(user_id: int, product_id: int) -> Dict[str, Any]:
    """Get features from Redis, similar to OnlineFeatureService."""
    try:
        # Construct the Redis key using user_id and product_id
        key = f"user:{user_id}:product:{product_id}"
        key_type = redis_client.type(key)
        print(f"Key type for {key} is {key_type}")

        # feature_data = redis_client.get(key)
        
        # if not feature_data:
        #     # Fallback to user-only features
        #     key = f"user:{user_id}:product:{product_id}"
        #     feature_data = redis_client.get(key)
        # CHI ERROR BRIEF: redis_client.get là dùng cho key, hgetall dùng cho hash
        feature_data = redis_client.hgetall(key)
        
        if not feature_data:
            # Fallback to user-only features
            key = f"user:{user_id}:product:{product_id}"
            feature_data = redis_client.hgetall(key)
            
        if not feature_data:
            return {"success": False, "error": "Features not found in Redis"}
        
        # CHI ERROR BRIEF: vì decode_responses=True  khi khởi tạo redis_client, 
        # thì tất cả các key-value trả về đã là str chứ không còn là bytes nữa.
        # features = json.loads(feature_data)
        features = feature_data  # Không cần decode lại
        return {"success": True, "features": features}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.on_event("startup")
async def startup_event():
    """Load the model when the API starts."""
    global loaded_model
    try:
        loaded_model, _ = load_model()
    except Exception as e:
        print(f"Warning: Could not load model at startup: {e}")

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "XGBoost Inference API is running"}

@app.post("/predict", response_model=PredictionResponse)
async def predict(requests: List[PredictionRequest]):
    """Make predictions using the loaded model."""
    try:
        # Ensure model is loaded
        model, model_file = load_model()
        print("Cac FEATURE DUNG DE TRAIN LA", model.feature_names)
        
        features = []
        
        # Get features for each request
        for request in requests:
            # Get features from Redis
            feature_result = get_features_from_redis(
                user_id=request.user_id, product_id=request.product_id
            )
            print(f"Feature result: {feature_result}")
            
            if not feature_result["success"]:
                raise HTTPException(
                    status_code=500,
                    detail=f"Loi la: {feature_result['error']} for user_id={request.user_id}, product_id={request.product_id}: {feature_result['error']}",
                )
            # CHI ERROR BRIEF: XGBoost không hỗ trợ kiểu dữ liệu Unicode (chuỗi) trực tiếp trong ma trận đầu vào,
            # có lỗ hổng khiến chuỗi (string) lọt vào mô hình, gây lỗi Unicode-2 is not supported của xgboost.
            # Redis trả về dạng (str) hết nha
            # Convert feature lists to single values
            # feature_dict = {}
            # for key, value in feature_result["features"].items():
            #     feature_dict[key] = value[0] if isinstance(value, list) else value // Vấn đề ở dòng này
            # Chi ép kiểu về float, lọc numeric
            feature_dict = {}
            for key, value in feature_result["features"].items():
                raw_value = value[0] if isinstance(value, list) else value
                try:
                    feature_dict[key] = float(raw_value)
                except (ValueError, TypeError):
                    # Gán giá trị mặc định nếu không phải số
                    feature_dict[key] = 0.0

            features.append(feature_dict)
        
        # Filter features to include only relevant columns
        filtered_features = [
            {key: feature.get(key, 0) for key in FEATURE_COLUMNS}
            for feature in features
        ]
        print("FILTERED_FEATURES", filtered_features)
        data=[[feature[col] for col in FEATURE_COLUMNS] for feature in filtered_features]
        print("DATA NHU NAY", data)

        # Create DMatrix for prediction
        dmatrix = xgb.DMatrix(
            data=[[feature[col] for col in FEATURE_COLUMNS] for feature in filtered_features],
            feature_names=FEATURE_COLUMNS
        )
        print("MONG LA KHONG LOI", dmatrix)
        # Make predictions
        predictions = model.predict(dmatrix)
        print("PREDICT DUOC ROI NE", predictions)
        return PredictionResponse(
            predictions=predictions.tolist(),
            success=True,
            model_file=os.path.basename(model_file)
        )
        
    except Exception as e:
        return PredictionResponse(
            predictions=[],
            success=False,
            model_file=os.path.basename(model_file),

            error=str(e)
        )

@app.get("/reload-model")
async def reload_model():
    """Force reload the latest model."""
    try:
        _, model_file = load_model()
        return {"success": True, "model_file": os.path.basename(model_file)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)