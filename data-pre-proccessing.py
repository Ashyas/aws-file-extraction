import base64
import boto3
import json
import gzip
import re

aws_access_key_id = base64.b64decode("QUtJQTJEUlFGREdLMzM1UVVFSFc=".encode()).decode()
aws_secret_access_key = base64.b64decode("MFVmMEVyc1gyRE9QRGtGVmdGM3FWZmM4T1AwQVRtem9zdjFTcmNZRA==".encode()).decode()

s3 = boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
bucket_name = "sample-log-data-028a9a4884211e5c6"
key = "example.log.gz"

obj = s3.Object(bucket_name, key)
with gzip.GzipFile(fileobj=obj.get()["Body"]) as gz_file:
    long_string = gz_file.read().decode()

pattern_to_exclude = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} [- \w*-?]+:? [\w+-?.?\[?,?\w?\]?]+:?'
matches = re.findall(pattern_to_exclude, long_string)

space_less_string = long_string.replace("  ", "").replace("\n", "").strip()
exclude = {'start': [], 'end': []}
json_strings = []

for match in matches:
    index = space_less_string.find(match)
    exclude['start'].append(index)
    exclude['end'].append(len(match) + index)

def add_json_strings(data):
    if data != '':
        json_format_string = str(json.loads(data)).replace("\'", "\"")
        json_strings.append(json_format_string)

for i in range(0, len(exclude['end'])):
    if i < len(exclude['end']) - 1:
        val = space_less_string[exclude['end'][i]:exclude['start'][i + 1]]
    else:
        val = space_less_string[exclude['end'][i]:]
    add_json_strings(val)

print(len(json_strings))
print(json_strings)

#**************Tests*******************

assert len(json_strings) == 21

first_json_string = json_strings[0]
assert first_json_string.startswith('{"FleetId": "fleet-xxx", "Errors": []')
assert first_json_string.endswith(', "RetryAttempts": 0}}')

last_json_string = json_strings[-1]
assert last_json_string.startswith('{"Resources": ["i-xxx"]')
assert last_json_string.endswith('"Value": "xxx-78"}]}')
