#Lấy dữ liệu từ redis để predict
import argparse
import redis
import kserve
from kserve import ModelServer,model_server
from typing import Dict

FEATURE_COLUMNS = [
    "brand", 
    "price", 
    "event_weekday", 
    "category_code_level1", 
    "category_code_level2", 
    "activity_count"
]

class RedisFeatureTransformer(kserve.Model):
    def __init__(self, model_name: str, predictor_host: str,  redis_host: str, redis_port: int):
        super().__init__(model_name)
        self.predictor_host = predictor_host
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.ready = True

    def preprocess(self, inputs: Dict, headers: Dict[str, str] = None):
        instances = inputs["instances"]
        transformed_instances = []
       
        for instance in instances:
            user_id = instance["user_id"]
            product_id = instance["product_id"]
            user_session = instance["user_session"]
            if not user_id:
                raise ValueError("Missing 'user_id' in input instance.")
            if not product_id:
                raise ValueError("Missing 'product_id' in input instance.")
            if not user_session:
                raise ValueError("Missing 'user_session' in input instance.")

            redis_key = f"user:{user_id}:product:{product_id}:session:{user_session}"
            key_type = self.redis_client.type(redis_key)
            features_data = self.redis_client.hgetall(redis_key)
            if not features_data:
                raise ValueError(f"No features found in Redis for key: {redis_key}")

            feature_dict = {}
            for key, value in features_data.items():
                raw_value = value[0] if isinstance(value, list) else value
                try:
                    feature_dict[key] = float(raw_value)
                except (ValueError, TypeError):
                    feature_dict[key] = 0.0

            feature_values = {
                key: feature_dict.get(key, 0.0)
                for key in FEATURE_COLUMNS
            }
            transformed_instances.append(feature_values)
  
            

        return {"instances": transformed_instances}

    def postprocess(self, inputs: Dict, headers: Dict[str, str] = None):
        return inputs

parser = argparse.ArgumentParser(parents=[model_server.parser])
parser.add_argument(
    "--redis_host", help="Redis host"
)
parser.add_argument(
    "--redis_port", help="Redis port", default=6379, type=int
)

args, _ = parser.parse_known_args()

if __name__ == "__main__":
    model = RedisFeatureTransformer(
        model_name = args.model_name, 
        predictor_host=args.predictor_host,
        redis_host=args.redis_host, 
        redis_port=args.redis_port)
    ModelServer(workers=1).start([model])