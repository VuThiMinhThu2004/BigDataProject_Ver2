from sklearn.datasets import make_regression
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

# Set MLflow tracking URI and experiment
mlflow.set_tracking_uri('http://10.200.2.51:5000')
mlflow.set_experiment('Exp-2')

with mlflow.start_run() as run:
    # Generate synthetic data for regression
    x, y = make_regression(n_features=4, n_informative=2, random_state=42, shuffle=False)
    
    # Split the data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
    
    # Define model parameters
    params = {'max_depth': 2, 'random_state': 42}
    model = RandomForestRegressor(**params)    
    
    # Fit the model
    model.fit(x_train, y_train)
    
    # Make predictions using the test data
    y_pred = model.predict(x_test)
    
    # Infer the signature of the model
    signature = infer_signature(x_test, y_pred)
    
    # Log the parameters and metrics
    mlflow.log_params(params)  # Log model parameters as a dictionary
    mlflow.log_metrics({"mse": mean_squared_error(y_test, y_pred)})  # Log Mean Squared Error (MSE) as a dictionary
    
    # Log the model and its signature
    mlflow.sklearn.log_model(sk_model=model,
                             artifact_path='sklearn-model',
                             signature=signature,
                             registered_model_name='sklearn-model-random-forest-reg')
