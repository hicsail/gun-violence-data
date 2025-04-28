import asyncio
import csv
import random  
import time  
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

GVA_DOMAIN = 'http://www.gunviolencearchive.org'

def _get_info(tr):
    tds = tr.find_all('td')
    if len(tds) < 8:
        raise ValueError(f"Expected 8 td cells, got {len(tds)}")

    def get_text(td):
        return td.get_text(strip=True)

    try:
        ID = get_text(tds[0])
        date = get_text(tds[1])
        state = get_text(tds[2])
        city_or_county = get_text(tds[3])
        address = get_text(tds[4])
        n_killed = int(get_text(tds[5]) or 0)
        n_injured = int(get_text(tds[6]) or 0)
        n_suspects_killed = int(get_text(tds[7]) or 0)
        n_suspects_injured = int(get_text(tds[8]) or 0)
        n_suspects_arrested = int(get_text(tds[9]) or 0)
    except Exception as parse_error:
        raise ValueError(f"Error parsing text fields: {parse_error}")

    try:
        incident_a = tds[10].find('a', string=lambda x: x and 'Incident' in x)
        incident_url = GVA_DOMAIN + incident_a['href'] if incident_a else ''

        source_a = tds[10].find('a', string=lambda x: x and 'Source' in x)
        source_url = source_a['href'] if source_a else ''
    except Exception as link_error:
        raise ValueError(f"Error extracting links: {link_error}")

    return date, state, city_or_county, address, n_killed, n_injured, n_suspects_killed, n_suspects_injured, n_suspects_arrested, incident_url, source_url

class Stage1Serializer:
    def __init__(self, output_fname, encoding='utf-8'):
        self._output_fname = output_fname
        self._encoding = encoding
        self._page_urls = []

    async def __aenter__(self):
        self._output_file = open(self._output_fname, 'w', encoding=self._encoding, newline='')
        self._writer = csv.writer(self._output_file)
        self._playwright = await async_playwright().start()
        
        # >>>: Random User-Agent to avoid detection
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Mozilla/5.0 (X11; Linux x86_64)",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
            "Mozilla/5.0 (iPad; CPU OS 13_2 like Mac OS X)"
        ]
        ua = random.choice(user_agents)
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._context = await self._browser.new_context(user_agent=ua)
        self._page = await self._context.new_page()
        return self

    async def __aexit__(self, type, value, tb):
        await self._page.close()
        await self._context.close()
        await self._browser.close()
        await self._playwright.stop()
        self._output_file.close()

    async def _gettext(self, url):
        # >>> : Add a random delay before making request
        delay = random.uniform(2, 6)
        print(f"Sleeping for {delay:.2f} seconds before visiting: {url}")
        await asyncio.sleep(delay)

        await self._page.goto(url, timeout=60000)
        return await self._page.content()

    async def _write_page(self, page_url):
        print(f"Fetching page: {page_url}")
        html = await self._gettext(page_url)
        soup = BeautifulSoup(html, 'html5lib')

        trs = soup.select('.responsive tbody tr')
        print(f"Found {len(trs)} rows in table")

        for tr in trs:
            try:
                info = _get_info(tr)
                self._writer.writerow(info)
            except Exception as e:
                print(f"Error parsing row: {e}")

    def write_header(self):
        self._writer.writerow([
            'date',
            'state',
            'city_or_county',
            'address',
            'n_killed',
            'n_injured',
            'n_suspects_killed',
            'n_suspects_injured',
            'n_suspects_arrested',
            'incident_url',
            'source_url'
        ])

    def write_batch(self, query_url, n_pages):
        batch = ['{}?page={}'.format(query_url, pageno) for pageno in range(n_pages - 1, 0, -1)] + [query_url]
        self._page_urls.extend(batch)

    async def flush_writes(self):
        print("Flushing writes made to serializer")
        for url in self._page_urls:
            await self._write_page(url)