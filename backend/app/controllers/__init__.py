from functools import wraps

from flask import session, redirect, url_for


def login_required(func):
    """
    This is a function that to reach some URLs by just logging to secure the URLs
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'email' in session:
            # User is logged in, allow access to the route
            return func(*args, **kwargs)
        else:
            # User is not logged in, redirect to login page
            return redirect(url_for('user_blueprint.login'))
    return wrapper
