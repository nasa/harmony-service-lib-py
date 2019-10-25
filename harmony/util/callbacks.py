import urllib

def _callback_do_post(harmony_message, path):
    """
    POSTs to the Harmony callback URL at the given path, which may include query params

    Parameters
    ----------
    harmony_message : object
        The Harmony input object.  See example/harmony-operation.json for the shape
    path : string
        The URL path relative to the Harmony callback URL which should be POSTed to

    Returns
    -------
    None
    """

    url = harmony_message['callback'] + path
    print('Starting response', url)
    request = urllib.request.Request(url, method='POST')
    print('Remote response:', urllib.request.urlopen(
        request).read().decode('utf-8'))
    print('Completed response', url)


def callback_with_redirect(harmony_message, redirect_url):
    """
    Performs a callback instructing Harmony to redirect the service user to the given URL

    Parameters
    ----------
    harmony_message : object
        The Harmony input object.  See example/harmony-operation.json for the shape
    redirect_url : string
        The URL where the service user should be redirected

    Returns
    -------
    None
    """
    _callback_do_post(harmony_message, '/response?redirect=%s' %
                     (urllib.parse.quote(redirect_url)))


def callback_with_error(harmony_message, message):
    """
    Performs a callback instructing Harmony that there has been an error and providing a
    message to send back to the service user

    Parameters
    ----------
    harmony_message : object
        The Harmony input object.  See example/harmony-operation.json for the shape
    message : string
        The error message to pass on to the service user

    Returns
    -------
    None
    """
    _callback_do_post(harmony_message, '/response?error=%s' %
                     (urllib.parse.quote(message)))

