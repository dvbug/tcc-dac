# coding: utf-8
"""
    tcc-dac.wsgi
    ~~~~~~~~~~~~

    tcc-dac website wsgi module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from dac.app import create_app
from . import config_override

application = create_app(config_override=config_override)

