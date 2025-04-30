import argparse
import kserve
import logging
from .transformer import RedisFeatureTransformer

logging.basicConfig(level=kserve.constants.KSERVE_LOGLEVEL)

parser = argparse.ArgumentParser(parents=[kserve.kfserver.parser])
DEFAULT_MODEL_NAME = "xgboost_model"

parser.add_argument(
    "--predictor_host",
    help="The URL for the model predict function", required=True
)
parser.add_argument(
    "--model_name", default=DEFAULT_MODEL_NAME,
    help='The name that the model is served under.')

parser.add_argument(
    "--redis_host", 
    help='Redis host', required=True)

parser.add_argument(
    "--redis_port", default = 6379,
    help='Redis port', type=int)

args, _ = parser.parse_known_args()
if __name__ == "__main__":
    transformer = RedisFeatureTransformer(
        name=args.model_name,
        predictor_host=args.predictor_host,
        redis_host=args.redis_host,
        redis_port=args.redis_port
    )
    kfserver = kserve.KFServer()
    kfserver.start(models=[transformer])