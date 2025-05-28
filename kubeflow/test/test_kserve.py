import requests

url = "http://35.247.171.74:8080/v1/models/bigdata-xgb:predict"
headers = {
    "Host": "bigdata-xgb.bigdata.example.com",
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IkRVczA2cC01RnRJRDIyUWxGRUk1aDF1RzhqS2thU0FIWU1yb1JFWUptRFUifQ.eyJhdWQiOlsiaXN0aW8taW5ncmVzc2dhdGV3YXkuaXN0aW8tc3lzdGVtLnN2Yy5jbHVzdGVyLmxvY2FsIl0sImV4cCI6MTc0OTQ2Mjg0MSwiaWF0IjoxNzQ4NDI2MDQxLCJpc3MiOiJodHRwczovL2t1YmVybmV0ZXMuZGVmYXVsdC5zdmMiLCJqdGkiOiJlNWRkNzI5OC0wZGZlLTRmMTUtYmVjZi0xZTEyYzJhMmJlMjciLCJrdWJlcm5ldGVzLmlvIjp7Im5hbWVzcGFjZSI6ImJpZ2RhdGEiLCJzZXJ2aWNlYWNjb3VudCI6eyJuYW1lIjoiZGVmYXVsdC1lZGl0b3IiLCJ1aWQiOiI3MjY5MGJhYy0zMDE3LTRlNDYtYWQ5Yi1lYmE1MzFlMjUxNzAifX0sIm5iZiI6MTc0ODQyNjA0MSwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmJpZ2RhdGE6ZGVmYXVsdC1lZGl0b3IifQ.Nwy8bnNCUsR2tqyBkHibMMmrCV3tztFQpsj1rkNTL-Cks4fkII7TfX3r-FAUPMmDCNEogijTZX4vJuIOwLw8eNCn-0VVi76FUKhKPt0E8oqdwFgTeMXYVs9rsQ5OjosKc_56Pcm0QI08eOCljnhcGfDzeoTlp0v-nWj75tNJfoK6OlbVJAVAAROHFxRHcGFNHNne5AKXdtI8v2Dw_Mj2AdUdQLekCbMcQ3tYNqR23BQiiK6gBeMebTq3VH-XkaGG8_NL5KRmTnINshGdywBGLcxr7h4IPBldkRx5lUp6iWaM3pURW8qnfCuCtVrQx1j_qIoEoOUqCZoY7KmQHMChxg"
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