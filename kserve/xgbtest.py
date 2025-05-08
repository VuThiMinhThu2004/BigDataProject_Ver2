FEATURE_COLUMNS = [
    "brand", 
    "price", 
    "event_weekday", 
    "category_code_level1", 
    "category_code_level2", 
    "activity_count"
]
feature_dict = {}
features_data = {
    "brand": "Nike",
    "price": 100.0,
    "event_weekday": 2,
    "category_code_level1": "Shoes",
    "category_code_level2": "Sports",
    "activity_count": 5
}
for key, value in features_data.items():
    raw_value = value[0] if isinstance(value, list) else value
    try:
        feature_dict[key] = float(raw_value)
    except (ValueError, TypeError):
        feature_dict[key] = 0.0

import xgboost as xgb
from load_latest import find_latest_model

ubj_path = find_latest_model()
model = xgb.Booster()
model.load_model(ubj_path)
feature_values = [feature_dict.get(key, 0.0) for key in FEATURE_COLUMNS]
print(feature_values)


data = xgb.DMatrix( data = [feature_values])
res = model.predict(data)
print(res)
