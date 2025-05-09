import requests

url = "http://localhost:80/v1/models/bigdata-xgboost:predict"
headers = {
    "Host": "bigdata-xgboost-predictor.kserve-test.example.com",
    "Content-Type": "application/json"
}


data = {

    "instances": [
        [{"brand": 0.0, "price": 0.0, "event_weekday": 999.99, "category_code_level1": 4.5, "category_code_level2": 1000, "activity_count": 0.0}],
        [{"brand": 0.0, "price": 10.0, "event_weekday": 999.99, "category_code_level1": 4.5, "category_code_level2": 1000, "activity_count": 0.0}]
    ]

}

response = requests.post(url, headers=headers, json=data)

print("Status code:", response.status_code)
print("Response body:", response.text)