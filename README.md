Wildfire Data Downloader
========================
This provides a python module containing a class to download data from the TIGGE dataset on the ECMWF MARS system for the Wildfire project.

Requirements
------------
The ECMWF API module must be present on the system you wish to run this on.  Instructions for installing the module can be found at <https://software.ecmwf.int/wiki/display/WEBAPI/Accessing+ECMWF+data+servers+in+batch#AccessingECMWFdataserversinbatch-python>.

It also requires the standard python modules `os`, `datetime`, `multiprocessing`, and `traceback`.

Usage
-----
Example usage:
```
from wildfire_retrieve import WildfireTiggeDataRetriever
from datetime import date
 
data_dir = '/path/to/data/dir'
ecmwf_keys = [('abcdefg-thisistheecmwfkey','email@domain.com')]
# Create a data downloader, with a target directory and the ECMWF keys
data_downloader = WildfireTiggeDataRetriever(data_dir, ecmwf_keys)

# This will attempt to download all of the data from 2007-03-05 (the first available date)
# up until the specified date.  If no date is supplied, all data up to today will be downloaded
# 
# This will not download data which already exists in the data_dir, unless the force argument is specified
data_downloader.bulk_download(date(2016,12,31))
```
