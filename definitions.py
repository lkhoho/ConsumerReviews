import os


PROJECT_ROOT = os.path.dirname(__file__)


def get_full_path(*paths):
    """
    Get the path relative to project root. E.g. get_full_path('dev', 'data.json') returns $PROJ/dev/data.json.
    Returns None if given paths are None.

    :param paths: path components.
    :return: complete path relative to project root. None if given paths are None.
    """

    if paths is None:
        return None

    return os.path.join(PROJECT_ROOT, *paths)
