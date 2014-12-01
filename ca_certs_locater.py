__file__ = 'ca_certs_locater.py'

import os


def get():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'cacerts.txt'))