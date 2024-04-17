from wsgiref.simple_server import make_server
from framework.application import Application
from route.route import route


application = Application(route)


if __name__ == '__main__':
    httpd = make_server('', 8009, application)
    httpd.serve_forever()

