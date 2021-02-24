import asyncio
import math
import numpy as np
import platform
import sys
import traceback as tb
import aiohttp
import json
import requests
import time
import random
import pandas as pd

from aiohttp import ClientResponse, ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientOSError, ClientResponseError
from aiohttp.hdrs import CONTENT_TYPE
from asyncio import CancelledError
from collections import namedtuple
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

from log_utils import log_first_call
from stage2_extractor import Stage2Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])
PROXY_URL = 'http://localhost:8191/v1'
# for toggling between userAgents
proxy_userAgent = {
    1: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleW...', 
    -1: 'Chrome/5.0 (Windows NT 10.0; Win64; x64) AppleW...'
}

def _compute_wait(average_wait, rng_base):
    log_first_call()
    log_average_wait = math.log(average_wait, rng_base)
    fuzz = np.random.standard_normal(size=1)[0]
    return int(np.ceil(rng_base ** (log_average_wait + fuzz)))

def _status_from_exception(exc):
    if isinstance(exc, CancelledError):
        return '<canceled>'
    if isinstance(exc, ClientOSError) and platform.system() == 'Windows' and exc.errno == 10054:
        # WinError: An existing connection was forcibly closed by the remote host
        return '<conn closed>'
    if isinstance(exc, asyncio.TimeoutError):
        return '<timed out>'

    return ''

class IpBlocked(Exception):
    pass

class Stage2Session(object):
    def __init__(self, **kwargs):        
        self.delay = 0
        self._extractor = Stage2Extractor()
        self._conn_options = kwargs
        self._proxy_sessId = None #for storing seesion Id of the proxy server 
        self._userAgent_index = 1 #for toggling between user agents

    #destroy session wih flare solver proxy 
    def __del__(self):               
        '''requests.post(PROXY_URL, data=json.dumps({
            "cmd": "sessions.destroy", 
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleW...",
            "maxTimeout": 60000, 
            "session": self._proxy_sessId
            })
        )'''

    async def __aenter__(self):        
        conn = TCPConnector(**self._conn_options)
        self._sess = await ClientSession(connector=conn).__aenter__()             
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    def _log_retry(self, url, status, retry_wait):
        print("GET request to {} failed with status {}. Trying again in {}s...".format(url, status, retry_wait), file=sys.stderr)

    def _log_extraction_failed(self, url):
        print("ERROR! Extraction failed for the following url: {}".format(url), file=sys.stderr)

    async def _get(self, url, average_wait=10, rng_base=2):       
        #initialize the proxy server session id if this is the first request 
        if self._proxy_sessId == None:              
            res = requests.post(PROXY_URL, data=json.dumps({"cmd": "sessions.create", "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleW...",
            "maxTimeout": 60000}))
            self._proxy_sessId = json.loads(res.text)['session']        

        payload =  {
            "cmd": "request.get",
            "url": url,
            "session": self._proxy_sessId, #keep the same session for every request 
            "userAgent": proxy_userAgent[self._userAgent_index],
            "maxTimeout": 60000                    
        }
        self._userAgent_index = self._userAgent_index * -1 # change userAgent for next request        
                
        while True:
            try:
                resp = await self._sess.post(PROXY_URL, data=json.dumps(payload), headers={"content-type": "application/json"})                
            except Exception as exc:
                status = _status_from_exception(exc)
                if not status:
                    raise
            else:
                status = resp.status
                if status < 500: # Suceeded or client error
                    return resp
                # It's a server error. Dispose the response and retry.
                await resp.release()

            wait = _compute_wait(average_wait, rng_base)
            self._log_retry(url, status, wait)
            await asyncio.sleep(wait)      
                
    def _get_fields_from_incident_url(self, row, driver):
        #time.sleep(self.delay)
        incident_url = row['incident_url']        
        #resp = await self._get(incident_url)

        time1 = time.time()
        driver.get(incident_url) 
        #Check to see if request is forbbiden due to IP block               
        if driver.exists_element(By.ID, 'content'):
            elem = driver.find_element_or_wait(By.ID, 'content')
            if 'with your ip and an explanation for why unusual traffic patterns were detected (if known)' in elem.get_attribute('innerHTML'): 
                raise IpBlocked                    
                #return 'FORBIDDEN_REQUEST'   
        elem = driver.find_element_or_wait(By.CSS_SELECTOR, '.region-content')
        time2 = time.time()        
        self.delay = random.uniform(1, 2) * (time2 - time1)       
        text = elem.get_attribute('innerHTML')                
        ctx = Context(address=row['address'],
                      city_or_county=row['city_or_county'],
                      state=row['state'])
        return self._extractor.extract_fields(text, ctx)

    def get_fields_from_incident_url(self, row, driver):        
        log_first_call()
        try:            
            return self._get_fields_from_incident_url(row, driver)
        except Exception as exc:
            if isinstance(exc, IpBlocked):
                raise                        
            pass
            # Passing return_exceptions=True to asyncio.gather() destroys the ability
            # to print them once they're caught, so do that manually here.
            '''if isinstance(exc, ClientResponseError) and exc.code == 404:
                # 404 is handled gracefully by us so this isn't too newsworthy.
                pass
            else:
                self._log_extraction_failed(row['incident_url'])
                tb.print_exc()
            raise'''
