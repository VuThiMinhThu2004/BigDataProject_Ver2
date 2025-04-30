import os
import glob
import re
from datetime import datetime



MODEL_DIR = "notebook/model-checkpoints/final-model/xgb_model"

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
