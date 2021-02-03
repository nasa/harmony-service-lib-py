from base64 import b64encode
import re
from urllib.parse import urlparse

from requests.auth import AuthBase
from requests import Session


# TODO: Is there a better way to ensure the auth is used on the EDL
# redirect only? I'd' prefer to get rid of the EarthdataSession and
# propagate the auth instance on the redirect request, relying on the
# EarthdataAuth._should_add_authorization_header helper to ensure its
# added only on the EDL request.

class EarthdataSession(Session):
    """Session which ensures the Authorization header is sent to correct servers"""
    def should_strip_auth(self, old_url: str, new_url: str) -> bool:
        urs_regex = r""".*urs\.earthdata\.nasa\.gov$"""
        old_hostname = urlparse(old_url).hostname
        old_tld = old_hostname.split('.')[-2:]
        new_hostname = urlparse(new_url).hostname
        new_tld = new_hostname.split('.')[-2:]

        same_tld = (old_tld == new_tld)
        to_edl = re.fullmatch(urs_regex, new_hostname) is not None

        return (not same_tld) and (not to_edl)


class EarthdataAuth(AuthBase):
    def __init__(self, edl_base_url, app_uid, app_pwd, user_access_token):
        self.edl_base_url = edl_base_url
        self.uid = app_uid
        self.pwd = app_pwd
        self.user_access_token = user_access_token

    def _should_add_authorization_header(self, r):
        # TODO: See above
        # edl_hostname = urlparse(self.edl_base_url).hostname
        # request_hostname = urlparse(r.url).hostname

        # return edl_hostname == request_hostname
        return True

    def _basic(self):
        creds = b64encode(f"{self.uid}:{self.pwd}".encode('utf-8'))
        creds = creds.decode('utf-8')
        return f"Basic {creds}"

    def _bearer(self):
        return f'Bearer {self.user_access_token}'

    def _auth_header(self):
        return ', '.join([
            self._basic(),
            self._bearer()
        ])

    def __call__(self, r):
        if self._should_add_authorization_header(r):
            r.headers['Authorization'] = self._auth_header()
        return r
