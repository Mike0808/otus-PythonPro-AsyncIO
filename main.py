# https://github.com/kentsommer/Async-Site-Crawler

import asyncio
import collections
import os
import aiofiles as aiof
import aiohttp
from aiohttp.client_exceptions import ClientConnectionError
from bs4 import BeautifulSoup
from urllib.parse import urlparse


async def read_url(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, ssl=False) as content:
                return await content.text()
        except: 
            pass


async def save_url(url: str, path: str, filename: str, id: str):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, ssl=False) as content:
                if content.content_type == 'text/html':
                    cont = await content.text()
                    path = os.path.join(path, filename)
                    async with aiof.open(path, 'w+') as out:
                        await out.write(cont)
                else:
                    path = os.path.join(path, 'image')
                    async with aiof.open(path, 'wb') as out:
                        await out.write(await content.read())
                print(f'done {id} - {url}')
        except: 
            pass



def is_valid_full_href(href: str):
    black_list = ['www.ycombinator.com/legal/', 'ycombinator.com/apply/', 'news.ycombinator.com']
    if not href:
        return False
    if not href.startswith('http'):
        return False
    for bad in black_list:
        if bad in href:
            return False
    return True


def find_urls(domain, content, id=0):
    result = collections.defaultdict(set)
    # result = []
    soup = BeautifulSoup(content, 'lxml')

    if not id:
        table = soup.find('table', attrs={'class': 'itemlist'})
        rows = table.find_all('tr')
        for row in rows:
            id = row.get('id')
            if id:
                for href in row.find_all('a'):
                    href = href.get('href')
                    if is_valid_full_href(href):
                        result[id].add(href)
    else:
        table2 = soup.find('table', attrs={'class': 'comment-tree'})
        if table2:
            rows2 = table2.find_all('tr')
            for row in rows2:
                for link in row.find_all('a'):
                    href = link.get('href')
                    if is_valid_full_href(href) and href not in result[id]:
                        result[id].add(href)
    return result


def path_parser(key, url):
    url_parsed = urlparse(url)
    filename = url_parsed.path.replace('/', '')
    path = os.path.join('./files/', key, url_parsed.hostname, filename)
    if not os.path.exists(path):
        os.makedirs(path, 0o777)
    return path


async def crawler(domain, uid=None, renew_cycle_time=0):
    while True:
        id, current = await q.get()
        print(current)
        content = await read_url(current)
        # urls = await find_urls(domain, content, id)
        urls = find_urls(domain, content, id).items()
        for key, value in urls:
            if len(value) == 1:
                if not value in seen:
                    seen.add(*value)
                    # q.put_nowait([key, f'{domain}/item?id={key}'])
                path = path_parser(key, *value)
                if path:
                    await save_url(str(*value), path, 'index.html', key)
            else:
                for item in value:
                    path = path_parser(key, item)
                    if path:
                        await save_url(item, path, 'index.html', key)

        q.task_done()
        if renew_cycle_time:
            await asyncio.sleep(renew_cycle_time)
            q.put_nowait([0, f'{domain}'])



async def run(n, site_domain):
    tasks = []
    try:
        for uid in range(n):
            task = asyncio.create_task(crawler(site_domain, uid))
            tasks.append(task)
        await asyncio.gather(*tasks)
        
        await q.join()
    except KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    


if __name__ == '__main__':
    domain = 'https://news.ycombinator.com'
    seen = set()
    num_workers = 4
    q = asyncio.Queue()
    q.put_nowait([0, f'{domain}'])

    event_loop = asyncio.get_event_loop()
    run = event_loop.run_until_complete(run(num_workers, domain))
    event_loop.close()
