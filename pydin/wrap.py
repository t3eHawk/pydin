"""Contains decorators used in application."""

import git
import functools


class BaseProperty():
    def __init__(self, func):
        self.func = func

    def __set__(self, obj, value):
        class_name = obj.__class__.__name__
        field_name = self.func.__name__
        full_name = f'property {field_name} of {class_name}'
        message = f'{full_name} cannot be changed this way'
        raise AttributeError(message)


class ClassProperty(BaseProperty):
    def __get__(self, obj, cls):
        return self.func(cls)


class StaticProperty(BaseProperty):
    def __get__(self, obj, cls):
        return self.func()


def classproperty(func):
    """Class property implementation."""
    return ClassProperty(func)


def staticproperty(func):
    """Static property implementation."""
    return StaticProperty(func)


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
