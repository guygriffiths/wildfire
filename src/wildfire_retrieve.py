#!/usr/bin/env python
'''

This module contains a single class to download the appropriate data for the Wildfire project from ECMWF.

It requires that the ecmwfapi module is installed.  Instructions for obtaining this API can be found at:

https://software.ecmwf.int/wiki/display/WEBAPI/Accessing+ECMWF+data+servers+in+batch#AccessingECMWFdataserversinbatch-python

@author: Guy Griffiths
'''

import os.path
from datetime import date, timedelta
from ecmwfapi import ECMWFDataServer
from multiprocessing import Pool, Manager
import traceback

class WildfireTiggeDataRetriever():
    ''' This class is a data downloader for data from the TIGGE dataset on the ECMWF MARS system.
    
    It requires a key/keys for the ECMWF API to be supplied.  You can obtain your ECMWF key by visiting:
    https://api.ecmwf.int/v1/key/
    
    You will need to have registered with ECMWF and will be asked to log in if you are not already logged in.
    
    You should make a note of the values given on that page for "Your registered email" and "Your api key".
    They will be needed when instantiating this class.
    '''
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
        self.lock = Manager().Lock()
    
    def bulk_download(self, end_date = None, start_date = None, force=False, reduced_set=False):
        ''' Download data in bulk.  This uses a multithreaded approach to make 4 simultaneous requests per API key,
        to speed up data retrieval.
        :param end_date: The date to download data up until.  Optional, defaults to today
        :param start_date: The first date to download data from.  Optional, defaults to 2007-03-05 (the first date available)
        :param force: Whether to download data which has already been downloaded.  Optional, defaults to False
        :param reduced_set: Whether to download a reduced set of fields to save space.  Optional, defaults to False
        :type end_date: datetime.date
        :type start_date: datetime.date
        :type force: boolean
        :type reduced_set: boolean
        '''
        # No date specified.  Try and download up to today.
        # This will fail on many of the more recent dates
        if end_date is None:
            end_date = date.today()
        # ECMWF allows 3 active requests per user, so we create a pool of 4 requests per user
        # to ensure that the pending requests get submitted immediately.
        # Could be set to 6, but 4 seemed to be quicker (not tested extensively though)
        size = len(self.keys) * 4
        worker_pool = Pool(size)
        if start_date is None:
            current_date = WildfireTiggeDataRetriever.start_date
        else:
            current_date = start_date
        args = []
        while current_date <= end_date:
            args.append((self,current_date.year, current_date.month, current_date.day, 0, self._get_ecmwf_key(), force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 6, self._get_ecmwf_key(), force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 12, self._get_ecmwf_key(), force, reduced_set))
            args.append((self,current_date.year, current_date.month, current_date.day, 18, self._get_ecmwf_key(), force, reduced_set))
            current_date += timedelta(days=1)
        # chunksize=1 means that we mostly keep the order of file retrieval intact
        # without it, we get downloads in a more random order, which is less efficient on MARS
        worker_pool.map(__get_data_wrapper__, args, chunksize=1)
    
    def get_data(self, year, month, day, hour, key, force=False, reduced_set=False):
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
            print 'Already downloaded',self._get_filename(year, month, day, hour, reduced_set),'not redownloading'
            return

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
        Cycles through the keys to maintain a balanced load on the requests.
        
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
    try:
        args[0].get_data(args[1], args[2], args[3], args[4], args[5], args[6], args[7])
    except:
        print('Problem with args: %s %s %s %s %s %s %s: %s' % (args[1],args[2],args[3],args[4],args[5],args[6],args[7],traceback.format_exc()))

if __name__ == '__main__':
    # Example usage
    data_dir = '/path/to/data/dir'
    ecmwf_keys = [('abcdefg-thisistheecmwfkey','email@domain.com')]
    # Create a data downloader, with a target directory and the ECMWF keys
    data_downloader = WildfireTiggeDataRetriever(data_dir, ecmwf_keys)
    
    # There are now 2 options.  You can download a specific file using the get_data method.
    # This will request the data from ECMWF.  The request will stay in a queue for an undetermined amount of time, then download
    # This method will block until the data is downloaded.
    # It will not download data which has already been retrieved, unless you add the force argument, e.g.:
    # data_downloader.get_data(2016, 10, 24, 0, force=True)
    data_downloader.get_data(2016, 10, 24, 0)
    
    # This is the preferred option.
    # It will attempt to download all of the data from 2007-03-05 (the first available date)
    # up until the specified date.  If no date is supplied, all data up to today will be downloaded
    # 
    # This will not download data which already exists in the data_dir, unless the force argument is specified
    data_downloader.bulk_download(date(2016,12,31))
