#!/usr/bin/env python

import atexit
import functools
import glob
import itertools
import json
import logging
import os
import random

import pi_1wire.sensors

from flask import Flask, request
from werkzeug.wrappers import Response

log = logging.getLogger(__name__)

sensors = pi_1wire.sensors.SensorManager(os.environ['SENSOR_CONFIG'])

def create_app(config_file):
    app = Flask("pi_1wire")
    app.config['DEBUG'] = True

    #app.logger.addHandler(logging.StreamHandler())
    app.logger.setLevel(logging.INFO)

    def start():
        global sensors
        #sensors.scan()

    def stop():
        global sensors
        sensors.stop_all()

    def json_io(wrapped_function):
        @functools.wraps(wrapped_function)
        def decorated_function(*args, **kwargs):
            if request.method != "GET" and request.content_type != "application/json":
                return Response(
                    json.dumps(
                        {"error": "Content-type must be application/json, not {}".format(request.content_type)},
                        indent=4
                    ),
                    status=405,
                    mimetype="application/json"
                )
            r = wrapped_function(*args, **kwargs)
            if isinstance(r, dict):
                return Response(
                    json.dumps(r),
                    mimetype="application/json"
                )
            if isinstance(r, tuple):
                if len(r) == 1 and isinstance(r[0], dict):
                    return Response(
                        json.dumps(r[0]),
                        mimetype="application/json"
                    )
                if len(r) == 2 and isinstance(r[0], dict):
                    return Response(
                        json.dumps(r[0]),
                        status=r[1],
                        mimetype="application/json"
                    )
            return Response(r, mimetype="application/json")
        return decorated_function


    @app.after_request
    def after_request(response):
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response


    def sensor_404(sensor):
        log.warn("sensor {} not found".format(sensor))
        return ({'error': 'sensor {} not found'.format(sensor)}, 404)


    @app.route("/")
    @app.route("/sensors/<string:sensor>", strict_slashes=False)
    @json_io
    def root(sensor=None):
        global sensors
        if sensor:
            if not sensor in sensors:
                return sensor_404(sensor)
            sensor_list = [sensor,]
        else:
            # handle both as comma delimited and as multiple values
            sensor_list = list(itertools.chain(*(v.split(",") for v in request.args.getlist('sensor') if v)))
        if sensor_list:
            if not any(s in sensors for s in sensor_list):
                log.warn("none of the following sensors exist: {}".format(", ".join(sensor_list)))
                return (
                    {
                        "error": "none of the following sensors exist: {}".format(", ".join(sensor_list))
                    },
                    404
                )
            # prune any sensors that we don't know about
            sensor_list = [s for s in sensor_list if s in sensors]
        else:
            sensor_list = sensors.keys()
        ###
        count = request.args.get('count', 0)
        if count:
            try:
                count = int(count)
            except:
                if count in ("all", "inf"):
                    count = float("inf")
                else:
                    count = 0
        ###
        return {s: sensors[s].last(count) for s in sensor_list}


    @app.route("/stats", strict_slashes=False)
    @app.route("/stats/<string:sensor>", strict_slashes=False)
    @json_io
    def stats(sensor=None):
        global sensors
        if sensor:
            if sensor in sensors:
                return {sensor: sensors[sensor].stats()}
            else:
                return sensor_404(sensor)
        return {s: sensors[s].stats() for s in sensors}

    @app.route("/rescan", strict_slashes=False)
    @json_io
    def rescan():
        global sensors
        added, resumed, suspended = sensors.scan()
        return {"added": added, "resumed": resumed, "suspended": suspended}

    start()
    atexit.register(stop)
    return app

def init_app(config=None):
    if not config:
        config = os.environ.get("PWM_CONFIG", "config.yaml")
    app = create_app(config)
    app.debug = True
    return app

if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(thread)d %(levelname)s %(message)s")
    logging.getLogger("").setLevel(logging.DEBUG)
    #app = init_app().test_client()
    app = init_app()
    #import pdb; pdb.set_trace()
    #app.run(host="::", port=8080)
