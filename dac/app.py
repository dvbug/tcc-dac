# coding: utf-8
from flask import Flask, url_for
from flask_restful import Api
from .common.util import JSONEncoder
from .resources.marey_diagram import TrainMareyDiagram, TrainMareyDiagramList
from .resources.line_header_config import LineHeaderConfig
from . import config


def create_app(config_override=None):
    app = Flask(__name__)
    app.json_encoder = JSONEncoder
    app.config.from_object(config)
    app.config.from_object(config_override)

    api = Api(app)

    # TODO register add_resource here
    api.add_resource(TrainMareyDiagramList, '/mareydiagrams/<string:date>/<string:line_no>/<string:plan_or_real>')
    api.add_resource(TrainMareyDiagram, '/mareydiagrams/<string:date>/<string:line_no>/<string:plan_or_real>/<trip>')
    api.add_resource(LineHeaderConfig, '/lineconfigs/<string:line_no>')
    return app
