import os
from gevent import os as gos


class DataStream(object):  # EXEMPT_FROM_CODE_COVERAGE
    def __init__(self, fd):
        self._fd = fd

    def write(self, buffer_to_write):
        return gos.tp_write(self._fd, buffer_to_write)

    def read(self, size):
        return gos.tp_read(self._fd, size)

    def close(self):
        try:
            if self._fd is not None:
                os.close(self._fd)
        except OSError:
            pass
        finally:
            self._fd = None
