"""Contains application constants."""

import platform


LINUX = True if platform.system() == 'Linux' else False
MACOS = True if platform.system() == 'Darwin' else False
WINDOWS = True if platform.system() == 'Windows' else False
