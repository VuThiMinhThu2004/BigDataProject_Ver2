# Cài Kserve

Vài câu lệnh kiểm tra
Kiểm tra node: kubectl get nodes
Kiểm tra tất cả pod: kubectl get pods --all-namespaces 
Kiểm tra namespaces: kubectl get namespaces


1. Bật K8S trong Docker Desktop
Vào Docker Desktop -> Vào cài đặt (Nút Settings ở góc phải) -> Kubernetes -> Tick vào enable Kubernetes -> Apply and restart

Chạy mấy bước sau yêu cầu để dành khá nhiều memory cho wsl2 nên cho chắc thì vào terminal 
   ```bash
   cd %UserProfile%

   notepad .wslconfig 
   ```
Thêm vào
   ```bash
[wsl2]
memory = 12GB (hoặc hơn nếu mấy bước sau vẫn bị k đủ memory tạo pod)
   ```
rồi restart máy

Tải các phần Kserve yêu cầu
2: Tải Custom Resource Definitions (dùng để định nghĩa tài nguyên cho k8s)
   ```bash
   kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.16.0/serving-crds.yaml  
   ```

3: Tải Knative Serving 
   ```bash
   kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.16.0/serving-core.yaml  
   ```

4: Tải cert-manager 
   ```bash
   kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.1/cert-manager.yaml 
   ```

5: Tải istio (quản lý network)
- Đợi bước 4 các pod cert-manager READY 1/1 hết thì mới chạy tiếp được
- Check:
+ xem các namespaces: kubectl get ns
+ xem các pod trong cert-manager: 
```bash
kubectl get pods -n cert-manager
PS C:\Users\dell\Documents\GitHub\BigDataProject_Ver2> kubectl get pods -n cert-manager
NAME                                      READY   STATUS    RESTARTS       AGE
cert-manager-cainjector-fb79858b4-hkd4w   1/1     Running   1 (168m ago)   27h
cert-manager-fbbb9fdd5-6kmqt              1/1     Running   3 (168m ago)   27h
cert-manager-webhook-6cc5985dd5-sghl7     1/1     Running   1 (168m ago)   27h
```

   
```bash
   kubectl apply -f https://github.com/knative/net-istio/releases/download/knative-v1.16.0/istio.yaml
```

6: Tích hợp Knative Serving với istio
   ```bash
kubectl apply -f https://github.com/knative/net-istio/releases/download/knative-v1.16.0/net-istio.yaml
   ```
7: Tải Kserve
```bash
kubectl apply -f https://github.com/kserve/kserve/releases/download/v0.13.1/kserve.yaml
```

Tải cluster resources cho KServe, đợi 1 lúc mới chạy được
  ```bash
kubectl apply -f https://github.com/kserve/kserve/releases/download/v0.13.1/kserve-cluster-resources.yaml
   ```

8: Kiểm tra port của istio
```bash
kubectl get svc istio-ingressgateway -n istio-system 
```

Lưu ý: nếu có bước nào lỗi thì kiểm tra toàn bộ pod xem có cái nào lỗi không READY không rồi chạy cái này để xem lỗi gì (nếu lỗi insufficient memory thì phải tăng memory của wsl2)
```bash
kubectl describe pod <tên pod> -n <tên namespace>
```


sửa dns mapping do svc.cluster.local không public qua ingress
đổi _example.com: thành example.com: |

```bash

kubectl edit cm config-domain --namespace knative-serving
```

Triển khai InferenceService
Tạo namespace kserve-test
```bash
kubectl create namespace kserve-test
```

config kết nối knative với minio host local, đổi địa chỉ IP 192.168.1.102 trong endpoint s3 "192.168.1.102:9000" thành địa chỉ IP của mình, tra địa chỉ IP thì dùng lệnh ipconfig
```bash
Ở dưới phần Wireless Lan adapter wi-fi:
Wireless LAN adapter Wi-Fi:

   Connection-specific DNS Suffix  . :
   Link-local IPv6 Address . . . . . : fe80::1d8c:d78d:3da2:2acd%14
   IPv4 Address. . . . . . . . . . . : 192.168.1.102
   Subnet Mask . . . . . . . . . . . : 255.255.255.0
   Default Gateway . . . . . . . . . : 192.168.1.1
```

```bash
kubectl apply -n kserve-test -f kserve-minio-secret_manual.yaml
```

Triển khai service trong namespace kserve-test
```bash
kubectl apply -n kserve-test -f inference.yaml

```
Kiểm tra pod sau khi chạy READY hết thì ok
```bash
kubectl get pods -n kserve-test

NAME                                                            READY   STATUS    RESTARTS   AGE
bigdata-xgboost-predictor-00001-deployment-cb7c9956c-ffbmz      2/2     Running   0          168m
bigdata-xgboost-transformer-00001-deployment-6bcb69b695-dglp9   2/2     Running   0          168m
```

Test request trong urlrequest.py




mấy câu lệnh giúp debug

kubectl get inferenceservices -n kserve-test  
kubectl describe inferenceservice test-minio -n kserve-test
kubectl get inferenceservices test-minio -n kserve-test
kubectl get revisions -n kserve-test
kubectl describe pod bigdata-xgboost-predictor-00001-deployment-64f64cc5cc-7m5dz -n kserve-test

Lấy service hostname
kubectl get inferenceservice bigdata-xgboost -n kserve-test -o jsonpath='{.status.components.predictor.url}' | sed 's|http://||'
bigdata-xgboost.kserve-test.example.com


Xóa service khi xong
kubectl delete -f test-minio.yaml -n kserve-test

docker build -t minhuet11/redis_transformer:latest -f redis_transformer.Dockerfile .

docker push minhuet11/redis_transformer:latest


kubectl delete -f inference.yaml -n kserve-test


kubectl get inferenceservice bigdata-xgboost -n kserve-test -o jsonpath='{.status.url}' | cut -d "/" -f 3