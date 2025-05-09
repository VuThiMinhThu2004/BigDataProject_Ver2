import xgboost as xgb
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
print(xgb.__version__)
# Load dataset
iris = load_iris()

X, y = iris.data, iris.target

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Convert to DMatrix (XGBoost's optimized data structure)
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

# Define parameters
params = {
    "objective": "multi:softmax",
    "num_class": 3,
    "eval_metric": "mlogloss"
}

# Train model
bst_model = xgb.train(params, dtrain, num_boost_round=10)
print(bst_model.feature_names)
# Save model
bst_model.save_model("xgboost_iris.ubj")

# Optional: Predict and evaluate
preds = bst_model.predict(dtest)
print("Accuracy:", accuracy_score(y_test, preds))