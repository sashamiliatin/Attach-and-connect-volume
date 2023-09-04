import logging
import functools

LOG = logging.getLogger(__name__)


class CleanUp(object):
    def __init__(self):
        self._actions = []

    def callOnExit(self, action, *args, **kwargs):
        self._actions.append((functools.partial(action, *args, **kwargs), False))
        return self

    def callOnException(self, action, *args, **kwargs):
        self._actions.append((functools.partial(action, *args, **kwargs), True))
        return self

    def __enter__(self):
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        while self._actions:
            action, cleanOnlyOnException = self._actions.pop()
            if (not cleanOnlyOnException) or exceptionType:
                try:
                    if exceptionType:
                        LOG.exception("performing cleanup due to exception: %s", exceptionValue)
                    action()
                except Exception as e:
                    LOG.exception("Cleanup action failed: %s", e)
