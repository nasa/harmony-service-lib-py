import ast
import io
import re
import os


def get_version():
    """
    Get the version of the currently running service lib. 

    Returns
    -------
    string
        A string representing the current version.
    """
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    main_file = os.path.join(cur_dir, "__init__.py")
    _version_re = re.compile(r"__version__\s+=\s+(?P<version>.*)")
    with open(main_file, "r", encoding="utf8") as f:
        match = _version_re.search(f.read())
        version = match.group("version") if match is not None else '"unknown"'
    return str(ast.literal_eval(version))