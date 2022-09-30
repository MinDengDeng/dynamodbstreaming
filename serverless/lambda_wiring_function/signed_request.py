import boto3
from collections import namedtuple
import json
import requests
from requests_aws4auth import AWS4Auth
import time

TransportResult = namedtuple('TransportResult', ['status', 'result_text', 'took_us', 'size'])

def send_signed(method, url, service='es', region='us-west-2', body=None):
    credentials = boto3.Session().get_credentials()
    auth=AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    result = None
    try:
        fn = getattr(requests, method)
        if body and not body.endswith("\n"):
            body += "\n"
        elif not body:
            body = ""
        start = time.time()
        result = fn(url, auth=auth, data=body, 
                    headers={"Content-Type":"application/json"})
        took = time.time() - start
        ret = TransportResult(status=int(result.status_code), result_text=result.text, 
                              took_us=took, size=len(body))
        return ret
    except Exception as e:
        msg = "Exception '{}'ing SignedRequest to {}. Message {}".format(method, url, e.message)
        return TransportResult(status=-1, result_text=msg, took_us=-1, size=-1)

def jsondecode(text):
    try:
        return json.JSONDecoder().decode(text)
    except Exception as e:
        str = "failed to decode JSON text %s" % text
        print(str)
    return ''
