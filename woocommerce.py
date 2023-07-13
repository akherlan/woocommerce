import httpx
import asyncio
from selectolax.parser import HTMLParser

from urllib.parse import urljoin
from random import randint

import logging
import json
import os
import re


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class WooClient(httpx.AsyncClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def get(self, url: str, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def request(self, method: str, url: str, **kwargs):
        retry_count = 5
        retry_delay = 1
        attempts = 0
        while attempts < retry_count:
            res = await super().request(method, url, **kwargs)
            if res and not res.is_error:
                return res
            attempts += 1
            await asyncio.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff
        return


class WooProductUrlCrawler:
    def __init__(self, url: str, all_product: bool = False, **kwargs):
        self.shop_url = urljoin(url, "shop/")
        self.all_product = all_product
        self.session = WooClient(follow_redirects=True, **kwargs)

    async def fetch(self):
        response = await self.session.get(self.shop_url)
        if response and not response.is_error:
            async for item in self.extract_link(response):
                yield item
                if self.all_product:
                    navs = self.extract_navigation(response)
                    for task in asyncio.as_completed(navs):
                        response = await task
                        if response and not response.is_error:
                            async for item in self.extract_link(response):
                                yield item

    async def extract_link(self, response):
        html = HTMLParser(response.text)
        a_product_selector = "a[class*=product__link]"
        for item in html.css(a_product_selector):
            product = item.attributes.get("href")
            yield product

    def extract_navigation(self, response):
        html = HTMLParser(response.text)
        a_nav_selector = "a[class=page-number]"
        navs = list(map(lambda p: p.text(strip=True), html.css(a_nav_selector)))
        navigation_requests = [
            self.session.get(urljoin(str(response.url), f"page/{i}/"))
            for i in range(2, int(navs[-1]) + 1)
        ]
        return navigation_requests

    async def aclose(self):
        await self.session.aclose()


class WooProductScraper:
    def __init__(self, **kwargs):
        self.session = WooClient(follow_redirects=True, **kwargs)
        self.feeds = None

    async def importurl(self, urlpath: str):
        with open(urlpath) as fin:
            self.feeds = [link.strip() for link in fin.readlines()]

    async def get_product(self):
        requests = [self.session.get(link) for link in self.feeds]
        for task in asyncio.as_completed(requests):
            response = await task
            await asyncio.sleep(randint(0, 3))
            async for product in self.parse_product(response):
                yield product

    async def parse_product(self, response):
        html = HTMLParser(response.text)
        try:
            json_scheme = html.css("script[type='application/ld+json']")
            if json_scheme:
                meta = []
                for item in json_scheme:
                    scheme = json.loads(item.text(strip=True), strict=False)
                    graph = scheme.get("@graph")
                    if graph:
                        product = list(
                            filter(lambda j: j.get("@type") == "Product", graph)
                        )
                        if len(product):
                            product = product[0]
                        bread = list(
                            filter(lambda j: j.get("@type") == "BreadcrumbList", graph)
                        )
                        meta.extend(*map(lambda k: k.get("itemListElement"), bread))
                meta = list(
                    filter(
                        lambda m: isinstance(m, dict),
                        map(lambda l: l.get("item"), meta),
                    )
                )
                product["breadcrumb"] = meta
        except Exception as e:
            logging.info("title: {}".format(html.css_first("title").text(strip=True)))
            logging.error(f"{str(e)} {str(response.url)}", exc_info=True)
            product = None
        finally:
            yield product

    def extract_description(self, html):
        try:
            description = html.css_first("div.product-short-description").html
        except Exception as e:
            description = None
            logging.error(str(e))
        finally:
            return description

    async def get_data(self, fileout: str):
        if re.findall(r"\/", fileout):
            directory = fileout.replace(fileout.split("/")[-1], "")
            if not os.path.exists(directory):
                os.makedirs(directory)
        f = open(fileout, "w")
        data = []
        async for product in self.get_product():
            data.append(product)
        json.dump(data, f)
        f.close()


if __name__ == "__main__":
    pass
