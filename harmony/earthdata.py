from base64 import b64encode
import re
from urllib.parse import urlparse

from requests.auth import AuthBase
from requests import Session

EDL_URL_PATTERN = r""".*urs\.earthdata\.nasa\.gov$"""


def _edl_url(url: str) -> bool:
    """Determine if the given URL is for Earthdata Login."""
    hostname = urlparse(url).hostname
    return re.fullmatch(EDL_URL_PATTERN, hostname) is not None


class EarthdataSession(Session):
    """Session which ensures the Authorization header is sent to correct
    servers.

    After instantiating the EarthdataSession, set its `auth` attribute
    to a valid EarthdataAuth instance:

        session.auth = EarthdataAuth(...)

    This lifecycle method on requests.Session is called when handling
    redirect requests. There are two cases important for handling
    Earthdata Login:

    (A) When handling a redirect from a resource server to Earthdata
    Login, the session will use the auth (if provided) to add the
    required Authorization to the request.

    (B) When handling a redirect from Earthdata Login back to a
    resource server, the session will remove the Authorization header
    from the request (which the `requests` package copies from the
    request which caused this redirect).

    """
    def rebuild_auth(self, prepared_request, response):
        # If not configured with an EarthdataAuth instance, defer to
        # default behavior
        if not self.auth:
            return super().rebuild_auth(prepared_request, response)

        if _edl_url(prepared_request.url):
            # (A) Defer to auth to add the Authorization header
            self.auth(prepared_request)
        else:
            # (B) Remove the Authorization header when redirecting away
            # from EDL.
            prepared_request.headers.pop('Authorization', None)


class EarthdataAuth(AuthBase):
    """Custom Earthdata Auth provider to add EDL Authorization headers to
    requests when required for token sharing and federated
    authentication.

    When instantiated with an EDL application's credentials and a
    user's access token, the resulting HTTP Authorization header will
    include the properly-encoded app credentials as a Basic auth
    header, and the user's access token as a Bearer auth header.

    """
    def __init__(self, app_uid: str, app_pwd: str, user_access_token: str):
        """Instantiate the Earthdata Auth provider.

        Parameters
        ----------
        app_uid:
            The Earthdata Login Application `uid`.

        app_pwd:
            The Earthdata Login Application `password`.

        user_access_token:
            The EDL-issued token for the user making the request.
        """
        creds = b64encode(f"{app_uid}:{app_pwd}".encode('utf-8')).decode('utf-8')
        self.authorization_header = f'Basic {creds}, Bearer {user_access_token}'

    def __call__(self, r):
        """The EarthdataAuth is a callable which adds Authorization headers
        when handling a request for Earthdata Login.

        """
        if _edl_url(r.url):
            r.headers['Authorization'] = self.authorization_header
        return r
