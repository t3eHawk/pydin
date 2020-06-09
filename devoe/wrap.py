"""Contains decorators used in application."""

import functools

import git


def check_repo(func):
    """Check if git repo exists and prevent access to API methods if not."""
    @functools.wraps(func)
    def wrapper(obj, *args, **kwargs):
        try:
            git.Repo(obj.jobs)
        except git.exc.InvalidGitRepositoryError:
            raise Exception(f'no git repo in {obj.jobs} found')
        else:
            func(obj, *args, **kwargs)
    return wrapper
