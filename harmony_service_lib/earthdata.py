from requests.auth import AuthBase
from requests import Session
class EarthdataSession(Session):
    """Session which ensures the Authorization header is sent to correct
    servers.

    After instantiating the EarthdataSession, set its `auth` attribute
    to a valid EarthdataAuth instance:

        session.auth = EarthdataAuth(...)

    This lifecycle method on requests.Session is called when handling
    redirect requests.
    """
    def rebuild_auth(self, prepared_request, response):
        # If not configured with an EarthdataAuth instance, defer to
        # default behavior
        if not self.auth:
            return super().rebuild_auth(prepared_request, response)

        self.auth(prepared_request)


class EarthdataAuth(AuthBase):
    """Custom Earthdata Auth provider to add EDL Authorization headers to
    requests when required for token sharing and federated
    authentication.

    When instantiated with an EDL application's credentials and a
    user's access token, the resulting HTTP Authorization header will
    include the properly-encoded app credentials as a Basic auth
    header, and the user's access token as a Bearer auth header.

    """
    def __init__(self, user_access_token: str):
        """Instantiate the Earthdata Auth provider.

        Parameters
        ----------
        user_access_token:
            The EDL-issued token for the user making the request.
        """
        self.authorization_header = f'Bearer {user_access_token}'

    def __call__(self, r):
        """The EarthdataAuth is a callable which adds Authorization headers
        when handling a request for sites backed by Earthdata Login.

        """
        r.headers['Authorization'] = self.authorization_header
        return r
