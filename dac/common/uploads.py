# coding:utf-8
"""
    tcc-dac.dac.common.uploads
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    uploads module based on flask.uploads ext.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from flaskext.uploads import configure_uploads, UploadSet


class DacFileUploads(object):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):

        configure_uploads(app, )