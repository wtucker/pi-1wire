#!/usr/bin/env python
import requests

BASE_URL = 'http://humblepi.tuckerlabs.com:8080'

class Temperature(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.sensors = self.active_sensors()

    def last(self, sensor_ids, count=1):
        """Get the last temperature for one or more sensors.
        
        Parameters
        ----------
        sensor_ids : string or list of strings
            0 or more sensor_ids.  If 0 sensor_ids are passed in, will return 
            the temp of all sensors.
        Returns
        -------
        dict
            Will be set or subset of requested sensor ids with the key being 
            the sensor id and the value an array of arrays of time, temp combos.  
        Notes
        -----
        May not return all requested sensor_ids if service does not know of 
        that sensor or there are no temperature reading for that sensor.
        """
        if isinstance(sensor_ids, basestring):
            sensors = sensor_ids
        else:
            sensors = ",".join(sensor_ids)
        resp = self.make_request('get',self.base_url,{'sensors':sensors, 'count':count})
        return resp

    def temp(self, sensor_id):
        """Get the temperature for one sensor.
        Parameters
        ----------
        sensor_id : string
            Sensor id for the sensor.
        Returns
        -------
        float
            The last temperature reading for the sensor.
        """
        resp = self.last(sensor_id)
        return resp[sensor_id][0][1]

    def active_sensors(self):
        return self.make_request('get', self.base_url, None).keys()

    def make_request(self, verb, url, params):
        verb = verb.lower()
        if verb == 'get':
            resp = requests.get(url, params=params)
        else:
            raise NotImplementedError("Verb not supported.")

        resp.raise_for_status()  #Will not throw something for 200
        return resp.json()

if __name__ == "__main__":
    t = Temperature(BASE_URL)
    
