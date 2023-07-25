#!/usr/bin/env python3

from scraper import WordpressScraper, Transformer
import argparse
import time


def main(url, product_file, offers_file, limit=None):
    tic = time.perf_counter()

    wordpress = WordpressScraper(url)
    wordpress.fetch(limit=limit)
    product, offers = wordpress.transform()
    wordpress.save(product, product_file)
    wordpress.save(offers, offers_file)

    toc = time.perf_counter()
    print(f"---------- {toc - tic:.2f} seconds ----------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraping product from a Wordpress Woocommerce powered website"
    )
    parser.add_argument("url", help="Wordpress website")
    parser.add_argument("-p", "--product", help="product file output")
    parser.add_argument("-o", "--offers", help="prices file output")
    parser.add_argument("-l", "--limit", help="limit products", type=int, default=None, const=None)
    args = parser.parse_args()
    main(url=args.url, product_file=args.product, offers_file=args.offers, limit=args.limit)