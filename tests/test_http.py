import pathlib

import pytest
import responses

import harmony.http
from harmony.http import (download, filename, is_http, optimized_url)
from tests.util import config_fixture

EDL_URL = 'https://uat.urs.earthdata.nasa.gov'


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


@pytest.fixture
def access_token(faker):
    return faker.password(length=40, special_chars=False)


@pytest.fixture
def validate_access_token_url():
    return (f'{EDL_URL}/oauth/tokens/user'
            '?token={token}&client_id={client_id}')


@pytest.fixture
def resource_server_granule_url():
    return 'https://resource.server.daac.com/foo/bar/granule.nc'


@pytest.fixture
def resource_server_redirect_url(faker):
    return ('https://n5eil11u.ecs.nsidc.org/TS1_redirect'
            f'?code={faker.password(length=64, special_chars=False)}'
            f'&state={faker.password(length=128, special_chars=False)}')


@pytest.fixture
def edl_redirect_url(faker):
    return ('https://uat.urs.earthdata.nasa.gov/oauth/authorize'
            f'?client_id={faker.password(length=22, special_chars=False)}'
            '&response_type=code'
            '&redirect_uri=https%3A%2F%2Fn5eil11u.ecs.nsidc.org%2FTS1_redirect'
            f'&state={faker.password(length=128, special_chars=False)}')


@responses.activate
def test_download_follows_redirect_to_edl_and_adds_auth_headers(
        monkeypatch,
        mocker,
        access_token,
        resource_server_granule_url,
        edl_redirect_url):

    monkeypatch.setattr(harmony.http, '_valid', lambda a, b: True)
    responses.add(
        responses.GET,
        resource_server_granule_url,
        status=302,
        headers=[('Location', edl_redirect_url)]
    )
    responses.add(
        responses.GET,
        edl_redirect_url,
        status=302
    )
    destination_file = mocker.Mock()
    cfg = config_fixture()

    response = download(cfg, resource_server_granule_url, access_token, None, destination_file)

    # We should get redirected to EDL
    assert response.status_code == 302
    assert len(responses.calls) == 2

    # We shouldn't have Auth headers in the request, but they should
    # be added on the redirect to EDL
    request_headers = responses.calls[0].request.headers
    redirect_headers = responses.calls[1].request.headers

    assert 'Authorization' not in request_headers
    assert 'Authorization' in redirect_headers
    assert 'Basic' in redirect_headers['Authorization']
    assert 'Bearer' in redirect_headers['Authorization']


@responses.activate
def test_download_follows_redirect_to_resource_server_with_code(
        monkeypatch,
        mocker,
        access_token,
        edl_redirect_url,
        resource_server_redirect_url):
    responses.add(
        responses.GET,
        edl_redirect_url,
        status=302,
        headers=[('Location', resource_server_redirect_url)]
    )

    monkeypatch.setattr(harmony.http, '_valid', lambda a, b: True)
    responses.add(
        responses.GET,
        resource_server_redirect_url,
        status=302
    )
    destination_file = mocker.Mock()
    cfg = config_fixture()

    response = download(cfg, edl_redirect_url, access_token, None, destination_file)

    assert response.status_code == 302
    assert len(responses.calls) == 2
    edl_headers = responses.calls[0].request.headers
    assert 'Authorization' in edl_headers
    rs_headers = responses.calls[1].request.headers
    assert 'Authorization' not in rs_headers


@responses.activate
def test_resource_server_redirects_to_granule_url(
        monkeypatch,
        mocker,
        access_token,
        resource_server_redirect_url,
        resource_server_granule_url):

    monkeypatch.setattr(harmony.http, '_valid', lambda a, b: True)
    responses.add(
        responses.GET,
        resource_server_redirect_url,
        status=301,
        headers=[('Location', resource_server_granule_url)]
    )
    responses.add(
        responses.GET,
        resource_server_granule_url,
        status=303
    )
    destination_file = mocker.Mock()
    cfg = config_fixture()

    response = download(cfg, resource_server_redirect_url, access_token, None, destination_file)

    assert response.status_code == 303
    assert len(responses.calls) == 2
    rs_headers = responses.calls[0].request.headers
    assert 'Authorization' not in rs_headers


@responses.activate
def test_download_validates_token(
        mocker,
        faker,
        access_token,
        validate_access_token_url,
        resource_server_granule_url):

    client_id = faker.password(length=22, special_chars=False)
    cfg = config_fixture(oauth_client_id=client_id)
    url = validate_access_token_url.format(
        token=access_token,
        client_id=client_id
    )

    responses.add(responses.POST, url, status=200)
    responses.add(responses.GET, resource_server_granule_url, status=200)
    destination_file = mocker.Mock()

    response = download(cfg, resource_server_granule_url, access_token, None, destination_file)

    assert response.status_code == 200
    assert responses.assert_call_count(url, 1) is True
    assert responses.assert_call_count(resource_server_granule_url, 1) is True


@responses.activate
def test_download_validates_token_once(
        mocker,
        faker,
        validate_access_token_url,
        resource_server_granule_url):

    client_id = faker.password(length=22, special_chars=False)
    access_token = faker.password(length=40, special_chars=False)
    cfg = config_fixture(oauth_client_id=client_id)
    url = validate_access_token_url.format(
        token=access_token,
        client_id=client_id
    )

    responses.add(responses.POST, url, status=200)
    responses.add(responses.GET, resource_server_granule_url, status=200)
    responses.add(responses.GET, resource_server_granule_url, status=200)
    destination_file = mocker.Mock()

    response = download(cfg, resource_server_granule_url, access_token, None, destination_file)
    response = download(cfg, resource_server_granule_url, access_token, None, destination_file)

    assert response.status_code == 200
    assert responses.assert_call_count(url, 1) is True
    assert responses.assert_call_count(resource_server_granule_url, 2) is True


@responses.activate
def test_download_validates_token_and_raises_exception(
        mocker,
        faker,
        validate_access_token_url):

    client_id = faker.password(length=22, special_chars=False)
    access_token = faker.password(length=42, special_chars=False)
    cfg = config_fixture(oauth_client_id=client_id)
    url = validate_access_token_url.format(
        token=access_token,
        client_id=client_id
    )

    responses.add(responses.POST, url, status=403, json={
        "error": "invalid_token",
        "error_description": "The token is either malformed or does not exist"
    })
    destination_file = mocker.Mock()

    with pytest.raises(Exception):
         download(cfg, 'https://xyzzy.com/foo/bar', access_token, None, destination_file)
         # Assert content


@pytest.mark.skip
def test_TODO_add_exception_handling_cases():
    pass


@pytest.mark.skip
def test_download_propagates_eula_error_message():
    pass


@pytest.mark.skip(reason='Feature request from EDL team')
def test_download_retries_correctly():
    # TODO: Feature request from EDL team
    #       On failure to validate or authenticate with EDL, add a
    #       handler to retry `n` times with increasing delay between
    #       each.
    pass
