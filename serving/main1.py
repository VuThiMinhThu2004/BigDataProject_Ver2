import os
import glob
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# Configuration
MODEL_DIR = r"G:\Documents\Github\BigDataProject\notebook\model-checkpoints\final-model\xgb_model"
FEATURE_COLUMNS = [
    "brand", "price", "event_weekday", 
    "category_code_level1", "category_code_level2", 
    "activity_count"
]

app = FastAPI(title="XGBoost Demo Inference API")

# Global model variable
loaded_model = None
current_model_file = None

class PredictionRequest(BaseModel):
    user_id: int = 530834332
    product_id: int = 1005073
    user_session: str = "040d0e0b-0a40-4d40-bdc9-c9252e877d9c"
    # Optional feature overrides for demo
    brand: Optional[str] = None
    price: Optional[float] = None
    event_weekday: Optional[int] = None
    category_code_level1: Optional[str] = None
    category_code_level2: Optional[str] = None
    activity_count: Optional[int] = None

class PredictionResponse(BaseModel):
    predictions: List[float]
    success: bool = True
    model_file: Optional[str] = None
    error: Optional[str] = None

def find_latest_model() -> str:
    # Same as in previous example
    pattern = os.path.join(MODEL_DIR, "xgboost_model_*.ubj")
    model_files = glob.glob(pattern)
    
    if not model_files:
        raise FileNotFoundError(f"No model files found in {MODEL_DIR}")
    
    latest_model = None
    latest_date = None
    
    for model_file in model_files:
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
    global loaded_model, current_model_file
    try:
        model_file = find_latest_model()
        
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

def get_demo_features(request: PredictionRequest) -> Dict[str, Any]:
    """Generate demo features based on user ID and any overrides."""
    # Default features - in a real system these would come from Redis
    default_features = {
        "brand": "samsung",
        "price": 899.99,
        "event_weekday": 2,
        "category_code_level1": "electronics",
        "category_code_level2": "smartphones",
        "activity_count": 5
    }
    
    # Override defaults with any provided values
    features = {**default_features}
    
    for field in FEATURE_COLUMNS:
        if hasattr(request, field) and getattr(request, field) is not None:
            features[field] = getattr(request, field)
    
    # Slightly vary features based on user_id and product_id for demo purposes
    user_factor = (request.user_id % 10) / 10
    product_factor = (request.product_id % 5) / 10
    
    features["price"] *= (1 + user_factor - product_factor)
    features["activity_count"] += int(request.user_id % 5)
    
    return {"success": True, "features": features}

@app.on_event("startup")
async def startup_event():
    """Load the model when the API starts."""
    try:
        _, _ = load_model()
    except Exception as e:
        print(f"Warning: Could not load model at startup: {e}")

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "XGBoost Demo Inference API is running"}

@app.post("/predict", response_model=PredictionResponse)
async def predict(requests: List[PredictionRequest]):
    try:
        # Ensure model is loaded
        model, model_file = load_model()
        
        features = []
        
        # Get features for each request
        for request in requests:
            feature_result = get_demo_features(request)
            
            if not feature_result["success"]:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Failed to generate features for request"
                )
            
            features.append(feature_result["features"])
        
        # Create DMatrix for prediction
        dmatrix = xgb.DMatrix(
            data=[[feature[col] for col in FEATURE_COLUMNS] for feature in features]
        )
        
        # Make predictions
        predictions = model.predict(dmatrix)
        
        return PredictionResponse(
            predictions=predictions.tolist(),
            success=True,
            model_file=os.path.basename(model_file)
        )
        
    except Exception as e:
        return PredictionResponse(
            predictions=[],
            success=False,
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
    uvicorn.run("demo_inference_api:app", host="0.0.0.0", port=8000, reload=True)