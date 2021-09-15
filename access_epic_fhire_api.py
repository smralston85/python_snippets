import json
import requests
from datetime import datetime, timedelta, timezone
from requests.structures import CaseInsensitiveDict
from jwt import (
    JWT,
    jwk_from_dict,
    jwk_from_pem,
)
from jwt.utils import get_int_from_datetime
import secrets

jwt_secret = secrets.token_hex(16)

instance = JWT()
message = {
    # Client ID for non-production
    'iss': 'confidential',
    'sub': 'confidential',
    'aud': 'https://fhir.epic.com/interconnect-fhir-oauth/oauth2/token',
    'jti': jwt_secret,        
    'iat': get_int_from_datetime(datetime.now(timezone.utc)),
    'exp': get_int_from_datetime(datetime.now(timezone.utc) + timedelta(minutes=3))
}

# Load a RSA key from a PEM file.
with open('confidential/RSA/privatekey.pem', 'rb') as fh:
    signing_key = jwk_from_pem(fh.read())

compact_jws = instance.encode(message, signing_key, alg='RS384')

headers = CaseInsensitiveDict()
headers['Content-Type'] = 'application/x-www-form-urlencoded'

data = {
  'grant_type': 'client_credentials',
  'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
  'client_assertion': compact_jws
}

x = requests.post('https://fhir.epic.com/interconnect-fhir-oauth/oauth2/token', headers=headers, data=data)
# print(x.text)

j_son = json.loads(x.content)

session_access = j_son['access_token']
