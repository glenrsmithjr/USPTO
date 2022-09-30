import gc, json, pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pandas.io.json import json_normalize
from pathlib import Path

ES_HOST = '192.168.10.105'
ES_PORT = 7020