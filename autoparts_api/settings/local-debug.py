from .local import *

DEBUG = True
INTERNAL_IPS = ['127.0.0.1', 'localhost']
INSTALLED_APPS.append('debug_toolbar')
MIDDLEWARE.append('debug_toolbar.middleware.DebugToolbarMiddleware')
