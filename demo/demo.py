import requests
import sys
import json

type_parameter = sys.argv[1]

def bulk():
    url = "https://hs8zfq7fek.execute-api.us-east-1.amazonaws.com/Default/start"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "input": {
            "duration": 120,
            "concurrent": 5,
            "interval": 0
        }
    }

    response = requests.post(url, json=data, headers=headers)
    print(response.text)

def single():
    url = "https://hs8zfq7fek.execute-api.us-east-1.amazonaws.com/Default/request"
    headers = {
        "Content-Type": "application/json"
    }
    with open("request.json", "r") as f:
        request = json.load(f)

    response = requests.post(url, json=request, headers=headers)
    print(response.text)

if type_parameter == "bulk":
    bulk()
elif type_parameter == "single":
    single()
else:
  print("Type parameter must be either 'bulk' or 'single'")