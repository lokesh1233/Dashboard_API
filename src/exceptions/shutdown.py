import os
from werkzeug.wsgi import ClosingIterator
import traceback

class ShutdownMiddleware:
    def __init__(self, app):
        self.app = app
        self.application = app.wsgi_app

    def post_request(self):
        if self.app.is_dead:
            os._exit(127)

    def __call__(self, environ, after_response):
        iterator = self.application(environ, after_response)
        try:
            return ClosingIterator(iterator, [self.post_request])
        except Exception:
            traceback.print_exc()
            return iterator