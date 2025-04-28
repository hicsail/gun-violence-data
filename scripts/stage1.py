#!/usr/bin/env python3
# stage 1: initial import of data fom gunviolencearchive.org using web scraping techniques

import asyncio
import dateutil.parser as dateparser
import logging as log
import platform
import sys
import warnings

import selenium_utils

from argparse import ArgumentParser
from calendar import monthrange
from datetime import date, timedelta
from functools import partial
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import parse_qs, urlparse

from stage1_serializer import Stage1Serializer

import random  
import time    
import pyautogui  

# Formats as %m/%d/%Y, but does not leave leading zeroes on the month or day.
DATE_FORMAT = '%#m/%#d/%Y' if platform.system() == 'Windows' else '%-m/%-d/%Y'

MESSAGE_NO_INCIDENTS_AVAILABLE = 'There are currently no incidents available.'

def random_sleep(min_sec=1.0, max_sec=3.0):
    """Sleep for a random amount of time between min_sec and max_sec."""  
    duration = random.uniform(min_sec, max_sec) 
    time.sleep(duration)  

def random_mouse_move(): 
    x, y = random.randint(100, 500), random.randint(100, 500)
    pyautogui.moveTo(x, y, duration=random.uniform(0.1, 0.5))

def parse_args():
    targets_specific_month = False
    if len(sys.argv) > 1:
        parts = sys.argv[1].split('-')
        if len(parts) == 2:
            targets_specific_month = True
            del sys.argv[1]

    parser = ArgumentParser()
    if not targets_specific_month:
        parser.add_argument('start_date', metavar='START', help="set start date", action='store')
        parser.add_argument('end_date', metavar='END', help="set end date", action='store')
        parser.add_argument('output_file', metavar='OUTFILE', help="set output file", action='store')

    parser.add_argument('-d', '--debug', help="show debug information", action='store_const', dest='log_level', const=log.DEBUG, default=log.WARNING)

    args = parser.parse_args()
    if targets_specific_month:
        month, year = map(int, parts)
        end_day = monthrange(year, month)[1]
        args.start_date = '{}-01-{}'.format(month, year)
        args.end_date = '{}-{}-{}'.format(month, end_day, year)
        args.output_file = 'stage1.{:02d}.{:04d}.csv'.format(month, year)
    return args

def query(driver, start_date, end_date):
    print("Querying incidents between {:%m/%d/%Y} and {:%m/%d/%Y}".format(start_date, end_date))
    random_sleep()  
    random_mouse_move()  

    driver.get('http://www.gunviolencearchive.org/query')
    random_sleep()  

    filter_dropdown_trigger = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-dropdown-trigger')
    driver.click(filter_dropdown_trigger)
    random_mouse_move()  
    random_sleep() 

    date_link = driver.find_element_or_wait(By.LINK_TEXT, 'Date')
    driver.click(date_link)
    random_mouse_move()  
    random_sleep()  

    input_date_from = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-from"]')
    input_date_to = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-to"]')
    start_date_str = start_date.strftime(DATE_FORMAT)
    end_date_str = end_date.strftime(DATE_FORMAT)
    script = '''
    arguments[0].value = "{}";
    arguments[1].value = "{}";
    '''.format(start_date_str, end_date_str)
    driver.execute_script(script, input_date_from, input_date_to)
    random_sleep()  

    old_url = driver.current_url
    form_submit = driver.find_element_or_wait(By.ID, 'edit-actions-execute')
    driver.click(form_submit)

    WebDriverWait(driver, 10).until(
        lambda d: d.current_url != old_url or d.find_elements(By.CSS_SELECTOR, 'table.views-table') or d.find_elements(By.CSS_SELECTOR, '.messages--warning'))

    updated_url = driver.current_url
    return updated_url, get_n_pages(driver)

def get_n_pages(driver):
    try:
        last_a = driver.find_element_or_wait(By.CSS_SELECTOR, 'a[title="Go to last page"]', timeout=1)
        last_url = last_a.get_attribute('href')
        form_data = urlparse(last_url).query
        n_pages = int(parse_qs(form_data)['page'][0]) + 1
        return n_pages
    except NoSuchElementException:
        tds = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive tbody tr td')
        if len(tds) == 1 and driver.get_value(tds[0]) == MESSAGE_NO_INCIDENTS_AVAILABLE:
            return 0
        return 1

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)

    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)

    step = timedelta(days=1)
    global_start, global_end = dateparser.parse(args.start_date), dateparser.parse(args.end_date)
    start, end = global_start, global_start + step - timedelta(days=1)

    async with Stage1Serializer(output_fname=args.output_file) as serializer:
        serializer.write_header()

        while start <= global_end:
            query_url, n_pages = query(driver, start, end)

            if n_pages > 0:
                serializer.write_batch(query_url, n_pages)

            start = end + timedelta(days=1)
            end = min(global_end, end + step)

        await serializer.flush_writes()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()