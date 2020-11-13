import unittest

import harmony.io
from harmony.util import config


class TestRequests(unittest.TestCase):
    def setUp(self):
        self.config = config(validate=False)

    def test_when_only_basic_auth_requested_it_creates_a_proper_auth_header(self):
        actual = harmony.io._auth_header(self.config, include_basic_auth=True)

        self.assertEqual(actual[0], 'Authorization')
        self.assertTrue('Basic' in actual[1])
        self.assertFalse('Bearer' in actual[1])

    def test_when_provided_an_access_token_it_creates_a_proper_auth_header(self):
        access_token = 'AHIGHLYRANDOMSTRING'
        expected = ('Authorization', f'Bearer {access_token}')

        actual = harmony.io._auth_header(self.config, access_token)

        self.assertEqual(expected, actual)

    def test_when_provided_an_access_token_and_basic_auth_requested_it_creates_a_proper_auth_header(self):
        access_token = 'AHIGHLYRANDOMSTRING'
        expected_bearer = f'Bearer {access_token}'

        actual = harmony.io._auth_header(self.config, access_token, include_basic_auth=True)

        self.assertEqual(actual[0], 'Authorization')
        self.assertTrue('Basic' in actual[1])
        self.assertTrue(expected_bearer in actual[1])

    def test_when_provided_an_access_token_it_creates_a_nonredirectable_auth_header(self):
        url = 'https://example.com/file.txt'
        access_token = 'OPENSESAME'
        expected_header = dict([harmony.io._auth_header(self.config, access_token)])

        actual_request = harmony.io._request_with_bearer_token_auth_header(self.config, url, access_token, None)

        self.assertFalse(expected_header.items() <= actual_request.headers.items())
        self.assertTrue(expected_header.items() <= actual_request.unredirected_hdrs.items())
