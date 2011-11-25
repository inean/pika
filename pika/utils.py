# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__date__ = '2011-04-06'

try:
    from collections import Callable
    CHECK_INSTANCE = True
except ImportError:
    CHECK_INSTANCE = False


def is_callable(handle):
    """
    Returns a bool value if the handle passed in is a callable method/function
    """
    if CHECK_INSTANCE:
        return isinstance(handle, Callable)

    return hasattr(handle, '__call__')


class cached(object):
    """
    Decorator that caches a function's return value each time it is
    called. If called later with the same arguments, the cached value
    is returned, and not re-evaluated.
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        try:
            return self.cache[args]
        except KeyError:
            value = self.func(*args)
            self.cache[args] = value
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(*args)

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return partial(self.__call__, obj)
