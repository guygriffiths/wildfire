#!/usr/bin/env python
'''
Created on 10 Nov 2016

@author: Guy Griffiths


Additional possible fields:

skin temperature
soil temperature
surface latent heat flux
surface net thermal radiation
surface sensible heat flux
total column water
mean sea level pressure
orography
snow fall water equivalent
soil moisture
surface net solar radiation
surface pressure
top net thermal radiation
'''

import os.path
from datetime import date, timedelta
from ecmwfapi import ECMWFDataServer
from multiprocessing import Pool

class WildfireTiggeDataRetriever():
    # The first available date in TIGGE
    start_date = date(2007,3,5)
    # These dates are not available.
    missing_dates = [(2015, 12, 3),(2015, 12, 10),(2015, 12, 16),(2015, 12, 17),(2015, 12, 18),
                     (2015, 12, 19),(2016, 6, 24),(2016, 6, 28),(2016, 6, 29),(2016, 6, 30),
                     (2016, 7, 1),(2016, 7, 2),(2016, 7, 3),(2016, 8, 6),(2016, 8, 10),
                     (2016, 8, 23),(2016, 8, 24),(2016, 8, 25),(2016, 8, 28)]
    
    # The set of parameter IDs for (in order):
    # 10m u wind component
    # 10m v wind component
    # 2m temperature
    # 2m dewpoint temperature
    # total precipitation
    minimal_vars = '165/166/167/168/228228'
    # The set of parameter IDs for all available variables which can be retrieved in a single request
    all_vars = '59/134/136/146/147/151/165/166/167/168/172/176/177/179/235/228001/228039/228139/228144/228228'
    
    def __init__(self, data_path, keys):
        ''' Instantiate a new WildfireTiggeDataRetriever
        :param data_path: The directory in which to store downloaded data
        :param keys: The ECMWF API email/API key pairs to use for data download.  Should be a list of tuples containing API key corresponding email address  
        :type data_path: string
        :type keys: list
        '''
        if not data_path.endswith('/'):
            data_path += '/'
        if not os.path.isdir(data_path):
            raise ValueError('data_path must be a directory')
        if len(keys) < 1:
            raise ValueError('Need to supply at least one ECMWF API key')
        self.path = data_path
        self.keys = keys
        self.current_key = 0
    
    def bulk_download(self, end_date = None, force=False, reduced_set=False):
        # No date specified.  Try and download up to today.
        # This will fail on many
        if end_date is None:
            end_date = date.today()
        # ECMWF allows 3 active requests per user, so we create a pool of 6 requests per user
        # to ensure that the pending requests get submitted immediately.
        size = len(self.keys) * 6
        worker_pool = Pool(size)
        current_date = WildfireTiggeDataRetriever.start_date
        args = []
        while current_date <= end_date:
            args.append((self,current_date.year, current_date.month, current_date.day, 0, force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 6, force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 12, force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 18, force, reduced_set))
            current_date += timedelta(days=1)
        worker_pool.map(__get_data_wrapper__, args)
    
    def get_data(self, year, month, day, hour, force=False, reduced_set=False):
        ''' Retrieves wildfire data for a given date + time.
        
        THIS METHOD RETRIEVES DATA SYNCHRONOUSLY AND WILL NOT RETURN UNTIL DATA IS DOWNLOADED.
        IT SHOULD BE CALLED IN ITS OWN THREAD FOR ALMOST ALL PRACTICAL USES
        
        Sorry for shouting, but it's important to note.  Generally, you will want to use the
        bulk_download method to get data.
        
        :param year: The desired year of the data to retrieve
        :param month: The desired month of the data to retrieve
        :param day: The desired day of the data to retrieve
        :param hour: The desired hour of the data to retrieve (0,6,12,18)
        :param force: Whether to download the data even if it already exists on disk
        :param reduced_set: Whether a reduced set of variables is being retrieved
        :type year: int
        :type month: int
        :type day: int
        :type hour: int
        :type force: boolean
        :type reduced_set: boolean
        
        :returns: Nothing.  Returns when data download is finished.
        '''
        # Only download if file doesn't exist and we haven't forced a redownload
        if not self.need_to_download(year, month, day, hour, reduced_set) and not force:
            print 'Already downloaded',self._get_filename(year, month, day, hour, reduced_set)
            print 'Set force=True to force a redownload'
            return
            
        key = self._get_ecmwf_key()
        server = ECMWFDataServer('https://api.ecmwf.int/v1',key[0], key[1])
        
        # Get the full set of variables?
        if reduced_set:
            variable_ids = WildfireTiggeDataRetriever.minimal_vars
        else: 
            variable_ids = WildfireTiggeDataRetriever.all_vars 
        
        # The dictionary for the ECMWF request
        request_params = {
            "class": "ti",
            "type": "cf",
            "dataset": "tigge",
            "expver": "prod",
            "grid": "0.5/0.5",
            "levtype": "sfc",
            "origin": "kwbc",
            "format": "netcdf",
            "step": "0/6/12/18/24/30/36/42/48/54/60/66/72/78/84/90/96/102/108/114/120/126/132/138/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240",
            "date": __format_date__(year, month, day),
            "param": variable_ids,
            "time": __format_time__(hour),
            "target": self._get_filename(year, month, day, hour, reduced_set),
            "area": "14/-82/-57/-31"
        }
        server.retrieve(request_params)
#         print 'Downloaded data:',self._get_filename(year, month, day, hour, reduced_set)
    
    def need_to_download(self, year, month, day, hour, reduced_set=False):
        ''' Will check whether or not the specified forecast needs to be downloaded.
        This will depend on whether or not it is a defined missing date, and whether the file
        already exists, and contains some data)
                
        :param year: The desired year of the data to retrieve
        :param month: The desired month of the data to retrieve
        :param day: The desired day of the data to retrieve
        :param hour: The desired hour of the data to retrieve (0,6,12,18)
        :param reduced_set: Whether a reduced set of variables is being retrieved
        :type year: int
        :type month: int
        :type day: int
        :type hour: int
        :type reduced_set: boolean
        
        :returns: Whether this data file needs to be download
        :rtype: boolean        
        '''
        # First check if this is an unavailable date. 
        if (year,month,day) in WildfireTiggeDataRetriever.missing_dates:
            return False
        # Now check if the file already exists and has a size > 0
        filename = self._get_filename(year, month, day, hour, reduced_set)
        if os.path.isfile(filename):
            if os.path.getsize(filename) > 0:
                return False
        return True
    
    def _get_ecmwf_key(self):
        ''' Gets the next ECMWF key ready to use.
        Cycles through the keys to maintain a balanced load on the requests
        
        :returns: The ECMWF key information
        :rtype: tuple containing (api_key, associated_email)
        '''
        key = self.keys[self.current_key]
        self.current_key += 1
        if self.current_key >= len(self.keys):
            self.current_key = 0
        return key
    
    def _get_filename(self, year, month, day, hour, reduced_set=False):
        ''' Gets the filename to use for the given variables.
        
        :param year: The desired year of the data to retrieve
        :param month: The desired month of the data to retrieve
        :param day: The desired day of the data to retrieve
        :param hour: The desired hour of the data to retrieve (0,6,12,18)
        :param reduced_set: Whether a reduced set of variables is being retrieved
        :type year: int
        :type month: int
        :type day: int
        :type hour: int
        :type reduced_set: boolean
        
        :returns: The target filename
        :rtype: string
        '''
        
        filename = self.path+__format_date__(year, month, day)+'T%02i-wildfire' % hour
        if reduced_set:
            filename += '-reduced.nc'
        else:
            filename += '.nc'
        return filename
    
    
def __format_date__(year, month, day):
    return "%04i-%02i-%02i" % (year,month,day)

def __format_time__(hour):
    return "%02i:00:00" % hour

def __get_data_wrapper__(args):
    # This just wraps the get_data function, because instance methods cannot be
    # used in a multiprocessing pool.  Wrapping them like this works fine
    # BUT NOT ON WINDOWS.
    args[0].get_data(args[1], args[2], args[3], args[4], args[5], args[6])

if __name__ == '__main__':
    data_dir = '/path/to/data/dir'
    ecmwf_keys =  [('abcdefg-thisismyecmwfkey','email@domain.com')]
    data_downloader = WildfireTiggeDataRetriever(data_dir, ecmwf_keys)
    data_downloader.bulk_download(date(2007,3,12))