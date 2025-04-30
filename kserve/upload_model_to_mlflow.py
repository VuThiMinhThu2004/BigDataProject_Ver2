import xgboost as xgb
import mlflow
import mlflow.xgboost
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from mlflow import MlflowClient
import os
import glob
import re
from datetime import datetime
import pandas as pd


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

#Load model
model_path = find_latest_model()
model = xgb.Booster()
model.load_model(model_path)

#Setup mlflow experiment and run to log model
mlflow.set_tracking_uri('http://localhost:5000/')
active_experiment = mlflow.set_experiment("log_model")
active_run = mlflow.start_run()
data_df = pd.read_csv('train_clean_small.csv')
model_artifact_path = 'xgboost_model'
mlflow.xgboost.log_model(model, artifact_path=model_artifact_path)


#Register Model
client = MlflowClient()
model_name = 'xgboost_final'
filter_string = f"name='{model_name}'"
results = client.search_registered_models(filter_string=filter_string)
if len(results) == 0:
    model_tags = {'framework': 'Xgboost'}
    model_description = 'Customer Behavior Prediction Model'
    client.create_registered_model(model_name, model_tags, model_description)
    
run_id = active_run.info.run_id
run_uri = f'runs:/{run_id}/{model_artifact_path}'
model_source = RunsArtifactRepository.get_underlying_uri(run_uri)
model_version = client.create_model_version(model_name, model_source, run_id)
mlflow.end_run()