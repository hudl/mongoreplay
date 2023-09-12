
# Python program to demonstrate
# Conversion of JSON data to
# dictionary

# importing the module
import json
import base64
from bson import BSON
import bson.json_util

results = []

# Opening JSON file
with open('writeErrorReplication.json') as json_file:
    for line in json_file:
        data = json.loads(line)
        results.append(data)

for result in results:
    if not result.get("rawop"):
        continue
    if not result.get("rawop").get("body"):
        continue
    if not result.get("rawop").get("body").get("$binary"):
        continue
    encoded_string = result["rawop"]["body"]["$binary"]["base64"]
    convertedbytes = base64.b64decode(encoded_string)
    print(convertedbytes)
    for line in convertedbytes:
        print(line)
        bson_obj = BSON(line)
        try:
            print(bson_obj.decode())
        except Exception:
            continue

