import os
os.environ['ENV'] = os.environ.get('ENV') or 'test'
os.environ['SHARED_SECRET_KEY'] = '_THIS_IS_MY_32_CHARS_SECRET_KEY_'
