from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from django.conf import settings


def build_google_service(service, version, scopes):
    private_key_path = settings.GOOGLE_PRIVATE_KEY_PATH
    credentials = ServiceAccountCredentials.from_json_keyfile_name(private_key_path, scopes)
    delegated_credentials = credentials.create_delegated(settings.DATA_EMAIL)
    http_auth = delegated_credentials.authorize(Http())
    return build(service, version, http=http_auth, cache_discovery=False)
