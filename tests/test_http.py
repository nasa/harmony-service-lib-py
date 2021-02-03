import pathlib

import pytest
import responses

from harmony.earthdata import EarthdataAuth, EarthdataSession
from harmony.http import filename, is_http, optimized_url

EDL_BASE_URL = 'https://uat.urs.earthdata.nasa.gov'


@pytest.mark.parametrize('url,expected', [
    ('http://example.com', True),
    ('HTTP://YELLING.COM', True),
    ('https://nosuchagency.org', True),
    ('hTTpS://topsecret.org', True),
    ('nothttp://topsecret.org', False),
    ('httpsnope://topsecret.org', False),
    ('s3://bucketbrigade.com', False),
    ('file:///var/log/junk.txt', False),
    ('gopher://minnesota.org', False)
])
def test_is_http(url, expected):
    assert is_http(url) is expected


@pytest.mark.parametrize('url', [
    'http://example.com/foobar.dos',
    'HTTP://YELLING.COM/loud.pdf',
    'https://nosuchagency.org/passwords.nsa',
    's3://bucketbrigade.com/pricing.aws',
    'file:///var/log/junk.txt'
])
def test_filename(url):
    directory = '/foo/bar'

    fn = str(filename(directory, url))

    assert fn.startswith(directory)
    assert fn.endswith(pathlib.PurePath(url).suffix)


@pytest.mark.parametrize('url,expected', [
    ('http://example.com/ufo_sightings.nc', 'http://example.com/ufo_sightings.nc'),
    ('http://localhost:3000/jobs', 'http://mydevmachine.local.dev:3000/jobs'),
    ('file:///var/logs/virus_scan.txt', '/var/logs/virus_scan.txt'),
    ('s3://localghost.org/boo.gif', 's3://localghost.org/boo.gif')
])
def test_when_given_urls_optimized_url_returns_correct_url(url, expected):
    local_hostname = 'mydevmachine.local.dev'

    assert optimized_url(url, local_hostname) == expected


@pytest.mark.skip
def test_download_validates_token():
    pass


@pytest.mark.skip
def test_download_validates_token_and_raises_exception():
    pass


@responses.activate
def test_download_follows_redirect_to_edl_and_adds_auth_headers():
    url = 'https://resource.server.daac.com/foo/bar/granule.nc'
    edl_redirect_url = ('https://uat.urs.earthdata.nasa.gov/oauth/authorize'
                        '?client_id=tiXkwDPzAkY1Xw55KBZeIw'
                        '&response_type=code'
                        '&redirect_uri=https%3A%2F%2Fn5eil11u.ecs.nsidc.org%2FTS1_redirect'
                        '&state=aHR0cDovL241ZWlsMTF1LmVjcy5uc2lkYy5vcmcvVFMxL0RQMC9PVEhSL05'
                        'JU0UuMDA0LzIwMTAuMDEuMTMvTklTRV9TU01JU0YxN18yMDEwMDExMy5IREZFT1M')

    responses.add(
        responses.GET,
        url,
        status=302,
        headers=[('Location', edl_redirect_url)]
    )
    responses.add(
        responses.GET,
        edl_redirect_url,
        status=302
    )

    auth = EarthdataAuth(EDL_BASE_URL, 'testappid1234', 'xyzzy3fizzbizz', '1234-5678-9012-3456')

    # TODO: replace with call to http.download...
    with EarthdataSession() as session:
        response = session.get(url, auth=auth)
        assert response.status_code == 302
        assert len(responses.calls) == 2
        assert 'Authorization' in responses.calls[1].request.headers


@pytest.mark.skip
def test_download_follows_redirect_to_resource_server_with_code():
    pass


@pytest.mark.skip
def test_download_receives_data_with_cookie_from_resource_server():
    pass


@pytest.mark.skip
def test_download_with_cookie_is_not_redirected_to_edl():
    pass


@pytest.mark.skip
def test_download_propagates_eula_error_message():
    pass


@pytest.mark.skip
def test_download_sets_a_timeout():
    pass


@pytest.mark.skip
def test_download_retries_correctly():
    pass
