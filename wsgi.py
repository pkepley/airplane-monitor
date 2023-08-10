'''
This app is used to run the dashboard.
Currently, I run gunicorn against this module.
'''
from app import app


application = app.server

if __name__ == '__main__':
    application.run()
