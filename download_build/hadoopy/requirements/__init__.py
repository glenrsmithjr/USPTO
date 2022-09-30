import json, os, pandas as pd, requests, sys

HADOOP_HOST = '192.168.10.105'
PORTS = {'namenode' : '7000', 'datanode1' : '7001', 'datanode2' : '7002', 'datanode3': '7003'}