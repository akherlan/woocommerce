#!/usr/bin/env python3

from woocommerce import WooProductUrlCrawler, WooProductScraper
import asyncio
import argparse
import time
import re


async def scrape_product_url(url: str, fileout: str, all_product: bool = False, **kwargs):
    woocrawler = WooProductUrlCrawler(url, all_product, timeout=60, **kwargs)
    with open(fileout, "a") as f:
        async for item in woocrawler.fetch():
            f.write(item + "\n")
            print(item)
    await woocrawler.aclose()


async def main(command, fin, fout):
    start = time.time()
    if command == "crawl":
        if not re.findall("^https?://(www)?.+", fin):
            print("input must be URL e.g. https://elizabeth.co.id")
        else:
            await scrape_product_url(url=fin, fileout=fout, all_product=True)
            print(f"---------- {time.time() - start} seconds ----------")
    elif command == "scrape":
        product_scraper = WooProductScraper(timeout=60)
        await product_scraper.importurl(urlpath=fin)
        await product_scraper.get_data(fileout=fout)
        print(f"---------- {time.time() - start} seconds ----------")
    else:
        print("must be 'crawl' or 'scrape'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraping product from a Woocommerce powered website"
    )
    parser.add_argument("fileout", help="output path for result")
    parser.add_argument("-c", "--command", help="crawl, scrape")
    parser.add_argument("-i", "--input", help="website, product links file path")
    args = parser.parse_args()
    asyncio.run(main(command=args.command, fin=args.input, fout=args.fileout))
