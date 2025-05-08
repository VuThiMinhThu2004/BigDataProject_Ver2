import requests

url = "http://localhost:80/v1/models/bigdata-xgboost:predict"
headers = {
    "Host": "bigdata-xgboost.kserve-test.example.com",
    "Content-Type": "application/json"
}
#"user_id": 572359107, "product_id": 16700058, "user_session": "ea1d92bf-eefc-412b-a8c5-ef4802fd37f7"
data = {

    "instances": [
        {"user_id": 572359107, "product_id": 16700058, "user_session": "ea1d92bf-eefc-412b-a8c5-ef4802fd37f7"}
    ]

}

response = requests.post(url, headers=headers, json=data)

print("Status code:", response.status_code)
print("Response body:", response.text)