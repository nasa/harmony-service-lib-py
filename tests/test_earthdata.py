from base64 import b64encode
from dataclasses import dataclass, field

import pytest
from requests import Session

from harmony.earthdata import EarthdataAuth, EarthdataSession


@dataclass
class FakeRequest:
    url: str = 'https://uat.urs.earthdata.nasa.gov/oauth'
    headers: dict = field(default_factory=dict)


@pytest.fixture
def earthdata_auth(faker):
    uid = faker.simple_profile('username')
    pwd = faker.password(length=12, special_chars=False)
    token = faker.password(length=40, special_chars=False)
    return EarthdataAuth(uid, pwd, token)


def test_authdata_auth_creates_correct_header(faker):
    uid = faker.simple_profile('username')
    pwd = faker.password(length=12, special_chars=False)
    token = faker.password(length=40, special_chars=False)
    auth = EarthdataAuth(uid, pwd, token)
    request = FakeRequest()

    auth(request)

    assert 'Authorization' in request.headers
    expected_creds = b64encode(f'{uid}:{pwd}'.encode('utf-8')).decode('utf-8')
    assert 'Basic' in request.headers['Authorization']
    assert expected_creds in request.headers['Authorization']
    assert 'Bearer' in request.headers['Authorization']
    assert token in request.headers['Authorization']


def test_earthdata_auth_given_edl_url_adds_auth_header(earthdata_auth):
    request = FakeRequest()

    earthdata_auth(request)

    assert 'Authorization' in request.headers


def test_earthdata_auth_given_non_edl_url_does_not_add_header(earthdata_auth):
    request = FakeRequest()
    request.url = 'https://github.com/acme/foobar'

    earthdata_auth(request)

    assert 'Authorization' not in request.headers


# Expected EarthdataSession behavior:
#
# Auth: False => delegates to super()
# Auth: True  =>
#       | Request Has Header | Request for EDL | Auth Header? |
#       |       False        |      False      |    False     |
#       |       False        |      True       |    True      |
#       |       True         |      False      |    False     |
#       |       True         |      True       |    True (*)  |
# Note:
#   (*) Replace pre-existing Authorization header with new header
#

def test_earthdata_session_given_no_auth_delegates_to_super(monkeypatch):
    called = False

    def mock_rebuild_auth(self, prepared_request, response):
        nonlocal called
        called = True
    monkeypatch.setattr(Session, 'rebuild_auth', mock_rebuild_auth)
    session = EarthdataSession()

    session.rebuild_auth(None, None)

    assert called


def test_earthdata_session_given_no_header_and_non_edl_url_request_does_not_contain_header(earthdata_auth):
    session = EarthdataSession()
    session.auth = earthdata_auth
    request = FakeRequest('https://duckduckgo.com/')

    session.rebuild_auth(request, None)

    assert 'Authorization' not in request.headers


def test_earthdata_session_given_no_header_and_edl_url_request_contains_new_header(earthdata_auth):
    session = EarthdataSession()
    session.auth = earthdata_auth
    request = FakeRequest()

    session.rebuild_auth(request, None)

    assert 'Authorization' in request.headers


def test_earthdata_session_given_header_and_non_edl_url_request_does_not_contain_header(earthdata_auth):
    session = EarthdataSession()
    session.auth = earthdata_auth
    request = FakeRequest('https://duckduckgo.com/', {'Authorization': 'PreExistingValue'})

    session.rebuild_auth(request, None)

    assert 'Authorization' not in request.headers


def test_earthdata_session_given_header_and_edl_url_request_contains_existing_header(earthdata_auth):
    session = EarthdataSession()
    session.auth = earthdata_auth
    request = FakeRequest(headers={'Authorization': 'PreExistingValue'})

    session.rebuild_auth(request, None)

    assert 'Authorization' in request.headers
    assert request.headers['Authorization'] != 'PreExistingValue'
