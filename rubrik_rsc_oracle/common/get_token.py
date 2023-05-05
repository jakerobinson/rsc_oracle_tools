import argparse
import pprint
import json
import requests


HTTP_ERRORS = {
    400: "Bad request: An error occurred while fetching the data",
    401: "Authentication error: Please provide valid credentials",
    403: "Forbidden: Please provide valid credentials",
    404: "Resource not found",
    500: "The server encountered an error"
}

pp = pprint.PrettyPrinter(indent=4)
parser = argparse.ArgumentParser()
parser.add_argument('-k', '--keyfile', dest='json_keyfile', required=True, help="JSON Keyfile", default=None)
parser.add_argument('--insecure', help='Deactivate SSL Verification', action="store_true")
args = parser.parse_args()
print("Keyfile: {}".format(args.json_keyfile))
print("Insecure: {}".format(args.insecure))
with open(args.json_keyfile) as f:
    json_key = json.load(f)

session_url = json_key['access_token_uri']
payload = {
    "client_id": json_key['client_id'],
    "client_secret": json_key['client_secret'],
    "name": json_key['name']
}
_headers = {
    'Content-Type': 'application/json;charset=UTF-8',
    'Accept': 'application/json, text/plain'
}
response = requests.post(
    session_url,
    json=payload,
    headers=_headers
)

if response.status_code != 200:
    if response.status_code in HTTP_ERRORS.keys():
        print(HTTP_ERRORS[response.status_code])

response_json = response.json()
if 'access_token' not in response_json:
    print("Access token not found")
    exit(1)
else:
    print("Bearer {}".format(response_json['access_token']))
    print("Service Account session created and Access Token has been obtained...")
exit()
