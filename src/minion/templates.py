import os
import os.path

from tornado import template


__all__ = ['loader']

path = os.path.dirname(__file__)
loader = template.Loader(os.path.join(path, 'templates'))
