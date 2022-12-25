import os

import tpmdata
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


tpmdata.tinit()

# You can generate a Token from the "Tokens Tab" in the UI
token = os.environ["INFLUXDB_V2_TOKEN"]
org = "apo"
bucket = "TPM"

client = InfluxDBClient(url="http://10.25.1.131:9999", token=token)

write_api = client.write_api(write_options=SYNCHRONOUS)

while 1:
    dd = tpmdata.packet(1, 0)

    keys = dd.keys()

    _points = []
    for item in keys:
        _points.append(Point("tpm").field(item, dd[item]))
    del dd
    for p in _points:
        write_api.write(bucket, org, p)
