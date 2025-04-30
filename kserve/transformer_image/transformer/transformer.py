#Lấy dữ liệu từ redis để predict

import kserve
import redis
import json
import os
import logging

logging.basicConfig(level=kserve.constants.KSERVE_LOGLEVEL)

class RedisFeatureTransformer(kserve.Model):
    def __init__(self, name: str, predictor_host: str,  redis_host: str, redis_port: int):
        super().__init__(name)
        self.predictor_host = predictor_host
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def preprocess(self, inputs: dict) -> dict:
        instances = inputs["instances"]
        transformed_instances = []

        for instance in instances:
            user_id = instance.get("user_id")
            product_id = instance.get("product_id")
            if not user_id:
                raise ValueError("Missing 'user_id' in input instance.")
            if not product_id:
                raise ValueError("Missing 'product_id' in input instance.")
            # Fetch features from Redis
            key = f"user:{user_id}:product:{product_id}"
            key_type = self.redis_client.type(key)
            features = self.redis_client.hgetall(key)
            
            if not features:
                raise ValueError(f"No features found in Redis for key: {key}")

            
            feature_values = [float(value) for value in features.values()]
            transformed_instances.append(feature_values)

        return {"instances": transformed_instances}

    def predict(self, inputs: dict) -> dict:
        # Forward the preprocessed inputs to the predictor
        return self._predict(inputs)

    def postprocess(self, inputs: dict) -> dict:
        # Pass-through postprocessing
        return inputs