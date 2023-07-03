import httpx
import asyncio
import time
import argparse
from random import randrange
from urllib.parse import urljoin
from selectolax.parser import HTMLParser


async def shop(response):
    html = HTMLParser(response.text)
    for item in html.css("a[class*=product__link]"):
        product = item.attributes.get("href")
        yield product


async def crawl(url, all_product=False):
    url = urljoin(url, "shop")
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60), follow_redirects=True
    ) as session:
        response = await session.get(url)
        html = HTMLParser(response.text)
        async for product in shop(response):
            yield product
        if all_product:
            navs = [page.text(strip=True) for page in html.css("a[class=page-number]")]
            other_pages = [
                session.get(str(response.url) + f"page/{i}/")
                for i in range(2, int(navs[-1]) + 1)
            ]
            for task in asyncio.as_completed(other_pages):
                new_response = await task
                time.sleep(randrange(2))
                async for next_product in shop(new_response):
                    yield next_product


async def scrape_product_url(url: str, fileout: str, all_product: bool = False):
    with open(fileout, "a") as f:
        async for item in crawl(url, all_product=True):
            f.write(item + "\n")
            print(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get list of product from a woocommerce website"
    )
    parser.add_argument("fileout", help="output path for result")
    parser.add_argument("-u", "--url", help="website url e.g. https://elizabeth.co.id")
    args = parser.parse_args()
    start = time.time()
    asyncio.run(
        scrape_product_url(url=args.url, fileout=args.fileout, all_product=True)
    )
    print(f"---------- {time.time() - start} seconds ----------")
