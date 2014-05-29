#!/usr/bin/env python

import logging
import threading
import time
import sys
import atexit
import collections
import random
import glob
import os

import yaml

from contextlib import closing, contextmanager
from collections import OrderedDict

log = logging.getLogger(__name__)


# use OrderedDicts instead of dicts when loading yaml
def dict_constructor(loader, node):
    return collections.OrderedDict(loader.construct_pairs(node))

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, dict_constructor)


class SensorError(RuntimeError):
    def __str__(self):
        if len(self.args) == 2:
            return "[Error {}] {}".format(*self.args)
        else:
            return str(self.args)

    @property
    def errno(self):
        if len(self.args) == 2:
            return self.args[0]
        else:
            return None


SENSOR_UNREADABLE = 1
SENSOR_FORMAT_INVALID = 2
SENSOR_CRC_INVALID = 4


class BaseSensor(threading.Thread):
    RECONFIGURABLES = ['name', 'max_history', 'interval']
    def __init__(
            self,
            name,
            max_history=3600,
            interval=30,
            *args,
            **kwargs
        ):
        super(BaseSensor, self).__init__(*args, **kwargs)
        self.name = name
        self.max_history = max_history
        self.interval = interval
        self.lock = threading.RLock()
        self._readings = collections.deque([], maxlen=max_history)
        self.shutdown = False
        self.daemon = True

    def reconfigure(self, **kwargs):
        for k, v in kwargs:
            if k in RECONFIGURABLES:
                c = getattr(self, k)
                if c != v:
                    log.info("[%s] changing %s from %s to %s", self.name, k, c, v)
                    setattr(self, k, v)
                else:
                    log.debug("[%s] %s is already set to %s", self.name, k, c)
            else:
                log.warn("[%s] unexpected parameter: %s = %s", k, v)

    @property
    def readings(self):
        with self.lock:
            return list(self._readings)

    def last(self, n=1):
        with self.lock:
            if n > len(self._readings):
                return list(self._readings)
            return [self._readings[i] for i in xrange(0-n, 0)]

    def stats(self):
        stats = {}
        with self.lock:
            valid_readings = [x[1] for x in self.readings if x[1]]
            valid_reading_count = len(valid_readings)
            total_reading_count = len(self.readings)
        stats["name"] = self.name
        stats["last"] = self.last()
        if valid_readings:
            stats["min"] = min(valid_readings)
            stats["max"] = max(valid_readings)
            stats["mean"] = sum(valid_readings) / float(valid_reading_count)
            stats["valid_ratio"] = (valid_reading_count / float(total_reading_count))
        else:
            stats["min"] = None
            stats["max"] = None
            stats["mean"] = None
            stats["valid_ratio"] = None
        stats["total_readings"] = total_reading_count
        stats["valid_readings"] = valid_reading_count
        return stats

    def retryable(self, ts):
        # TODO: do something more intelligent than just seeing if we're
        # more than half way to the next interval
        return time.time() < float(ts + self.ts(offset=self.interval))/2

    def sleep_time(self):
        return self.ts(offset=self.interval) - time.time()

    def run(self):
         self.shutdown = False
         self.suspended = False
         sleep_time = self.sleep_time()
         log.debug("[%s] sleeping for %f seconds", self.name, sleep_time)
         time.sleep(sleep_time)
         while not self.shutdown:
             if self.suspended:
                 sleep_time = self.sleep_time()
                 log.debug("[%s] paused; sleeping for %f seconds", self.name, sleep_time)
                 time.sleep(sleep_time)
                 continue
             t0 = time.time()
             ts_ = self.ts()
             reading = None
             retries = 0
             while reading is None:
                 try:
                     reading = self._read()
                     break
                 except SensorError as e:
                     if e.errno != SENSOR_CRC_INVALID:
                         log.warn("[%s] non-retryable error: %s", self.name, repr(e))
                         break
                     if retries:
                         log.info("[%s] retry %d caught %s", self.name, retries, repr(e))
                         retries += 1
                     else:
                         log.debug("[%s] first try caught %s", self.name, repr(e))
                         retries += 1
                     if not self.retryable(ts_):
                         log.warn("[%s] insufficient time to retry", self.name)
                         break

             ts = self.ts()
             slips = (ts - ts_) / self.interval
             if slips:
                 log.warn("[%s] internal slip of %d intervals detected", self.name, slips)

             with self.lock:
                 self._readings.append([ts, reading])

             if not slips:
                 sleep_time = self.sleep_time()
                 log.debug("[%s] sleeping for %f seconds", self.name, sleep_time)
                 time.sleep(sleep_time)

    def stop(self):
        self.shutdown = True

    def ts(self, offset=0):
        return self.interval * int((time.time() + offset) / self.interval)


class MockSensor(BaseSensor):
    RECONFIGURABLES = BaseSensor.RECONFIGURABLES + ['generator', 'initial', 'low', 'high', 'down_delta', 'up_delta']
    def __init__(self, name, generator=None, initial=20, low=10, high=30, down_delta=1, up_delta=1, preseed=3600, *args, **kwargs):
        super(MockSensor, self).__init__(name, *args, **kwargs)
        self.name = name
        if generator:
            self.generator = generator
        else:
            self.generator = MockSensor.bounded_generator(initial, low, high, down_delta, up_delta)
        self.preseed = min(preseed, self.max_history)
        self._preseed()

    def _preseed(self):
        ts = self.ts()
        with self.lock:
            for i in xrange(ts-(self.preseed+1)*self.interval, ts, self.interval):
                self._readings.append([ts, self._read()])

    def _read(self):
        return self.generator.next()

    @staticmethod
    def bounded_generator(init, low, high, down_delta=1, up_delta=1):
        value = init
        down_delta = 0 - abs(down_delta)
        up_delta = abs(up_delta)
        yield value
        while True:
            delta = random.uniform(max(low-value, down_delta), min(high-value, up_delta))
            value = value + delta
            yield value


class SysFSSensor(BaseSensor):
    RECONFIGURABLES = BaseSensor.RECONFIGURABLES + ['serial']
    def __init__(self, name, serial, *args, **kwargs):
        super(SysFSSensor, self).__init__(name, *args, **kwargs)
        self.serial = serial
        self.in_error = False

    def get_serial(self):
        return self._serial

    def set_serial(self, serial):
        self._serial = serial
        self.path = "/sys/bus/w1/devices/{}/w1_slave".format(serial)

    serial = property(get_serial, set_serial)

    def _read(self):
        try:
            with closing(file(self.path)) as fh:
                data = fh.read()
                lines = data.splitlines()
                # good CRC:
                # c4 01 4b 46 7f ff 0c 10 3b : crc=3b YES
                # c4 01 4b 46 7f ff 0c 10 3b t=28250
                # bad CRC:
                # ff ff ff ff ff ff ff ff ff : crc=c9 NO
                # 8c 01 4b 46 7f ff 04 10 2e t=-62
                if len(lines) != 2:
                    raise SensorError(
                        SENSOR_FORMAT_INVALID,
                        "expected 2 lines but got %d", len(lines)
                    )
                if lines[0][-3:] != 'YES':
                    raise SensorError(
                        SENSOR_CRC_INVALID,
                        "CRC invalid: {}".format(lines)
                    )
                try:
                    eq_index = lines[1].rindex("=")
                except ValueError:
                    raise SensorError(
                        SENSOR_FORMAT_INVALID,
                        "could not find index for '=' symbol in {}".format(lines[1])
                    )
                try:
                    return float(lines[1][eq_index+1:]) / 1000
                except ValueError as exc:
                    raise SensorError(
                        SENSOR_FORMAT_INVALID,
                        str(exc)
                    )
        except IOError as e:
            raise SensorError(
                SENSOR_UNREADABLE,
                "unreadable: {}".format(str(e))
            )


class SensorManager(object):
    def __init__(self, config_file):
        self._config_file = config_file
        self._sensors = OrderedDict()
        self._by_tag = {}
        try:
            self._load_config()
        except Exception as e:
            log.critical("failed to load configuration", exc_info=1)
            raise
        self._init_sensors()

    def _load_config(self):
        config = yaml.load(file(self._config_file))
        for sensor, sensor_config in config['sensors'].iteritems():
            sensor_class = getattr(sys.modules[__name__], sensor_config['class'])
            sensor_config['_class'] = sensor_class
            if not isinstance(sensor_config.get('tags', []), (list, tuple)):
                raise TypeError(
                    "'tags' property for {} is of type {} but should be a list"
                    .format(sensor, type(sensor_config.get('tags')))
                )
            if not isinstance(sensor_config.get('args', {}), (dict)):
                raise TypeError(
                    "'args' property for {} is of type {} but should be a dict"
                    .format(sensor, type(sensor_config.get('args')))
                )
        self._config = config

    def _init_sensors(self):
        for sensor, sensor_config in self._config['sensors'].iteritems():
            self[sensor] = sensor_config['_class'](name=sensor, **sensor_config.get('args', {}))
            self[sensor].start()

    def stop_all(self):
        for sensor in self._sensors.itervalues():
            sensor.stop()

    def __contains__(self, key):
        return key in self._sensors

    def __getitem__(self, key):
        return self._sensors[key]

    def __setitem__(self, key, value):
        self._sensors[key] = value

    def keys(self):
        return self._sensors.keys()


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(module)s %(message)s")
    log.setLevel(logging.DEBUG)
    m = SensorManager(os.environ['SENSOR_CONFIG'])

    def qq():
        log.setLevel(logging.CRITICAL)

    def q():
        log.setLevel(logging.WARN)

    def v():
        log.setLevel(logging.INFO)

    def vv():
        log.setLevel(logging.DEBUG)

