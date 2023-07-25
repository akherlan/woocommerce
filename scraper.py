import httpx
from selectolax.parser import HTMLParser
import pandas as pd
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta, timezone
from random import randrange
from time import sleep
import logging
import json
import re


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DEFAULT_HEADERS = {"user-agent": "httpx"}
DEFAULT_TIMEOUT = 45  # second
BRAND_LIST_TEXT = "wordpress.txt"


class WordpressScraper:
    def __init__(self, website, **kwargs):
        self.session = httpx.Client(
            headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT, **kwargs
        )
        self.website = website
        self.rawdata = None

    def fetch(self, limit=None):
        """All product listing are available from Wordpress REST API
        no need to crawl page by page
        read more: https://developer.wordpress.org/rest-api/
        """
        product_endpoint = "wp-json/wp/v2/product"
        url = urljoin(self.website, product_endpoint)

        if limit is None:
            limit = 999999
            lim = 100
        elif limit > 100:
            lim = 100
        else:
            lim = limit

        p = 1
        last = False
        data = []

        while not last:
            par = {"per_page": lim, "page": p}
            response = self.session.get(url, params=par)
            logging.info("GET page {}".format(str(p)))
            if response.status_code == 200:
                data += response.json()
                total_pages = response.headers.get("x-wp-totalpages")
                last = p >= int(total_pages) or len(data) >= limit
                p += 1
            else:
                logging.error("bad response {}".format(response.status_code))
                return

        if len(data) > limit:
            data = data[:limit]
        logging.info("collect {} items from {}".format(len(data), self.website))
        self.rawdata = data
        return data

    def transform(self):
        transformer = Transformer(self.rawdata)
        product, offers = transformer.generate_dataset()
        return product, offers

    def save(self, dataset, fname):
        dataset.to_csv(fname, index=False)


class Transformer:
    def __init__(self, rawdata, **kwargs):
        self.session = httpx.Client(
            headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT, **kwargs
        )
        self.rawdata = self.import_rawdata(rawdata)

    def import_rawdata(self, rawdata):
        return list(filter(lambda item: item.get("status") == "publish", rawdata))

    def brand_list(self):
        with open(BRAND_LIST_TEXT) as fin:
            brand = [line.strip().split(",") for line in fin.readlines()]
            return {item[0]: item[1] for item in brand}

    def define_brand(self, item):
        domain = urlparse(self.get_website(item)).netloc
        return self.brand_list().get(domain)

    def get_website(self, item):
        domain = urlparse(item.get("link"))
        return f"{domain.scheme}://{domain.netloc}"

    def get_sku(self, item):
        name = item.get("slug").split("-")
        digit = [string for string in name if string.isdigit()]
        if digit:
            return digit[-1]
        else:
            logging.warning("missing sku {}".format(item.get("link")))
            return

    def get_clean_name(self, item):
        return (
            item.get("title").get("rendered").replace("&#8211;", "-").replace("–", "-")
        )

    def get_category(self, item):
        url = list(
            filter(
                lambda x: x.get("taxonomy") == "product_cat", item["_links"]["wp:term"]
            )
        )[0]["href"]
        response = self.session.get(url)
        if response.status_code == 200:
            logging.info(
                "GET category for {}".format(item.get("title").get("rendered"))
            )
            return ",".join([cat.get("name").title() for cat in response.json()])
        else:
            logging.warning("GET category failed {}".format(item.get("link")))
            return

    def extract_variation_json(self, html):
        varobj = html.css_first("form[enctype='multipart/form-data']")
        if varobj is not None:
            varstring = varobj.attributes.get("data-product_variations")
            if varstring is not None:
                return json.loads(varstring)
        else:
            return

    def get_offers(self, item):
        link = item.get("link")
        response = self.session.get(link)
        if response.status_code != 200:
            logging.error("bad response {} {}".format(response.status_code), link)
            return
        logging.info("GET price {}".format(link))
        html = HTMLParser(response.text)
        brand_list = list(self.brand_list().values())
        brand_list.remove("This is April")
        if self.define_brand(item) in brand_list:
            varjson = self.extract_variation_json(html)
            if varjson is not None:
                values = []
                for key in ("sku", "display_price", "is_instock", "variation_description"):
                    value = list(map(lambda item: item.get(key), varjson))
                    values.append(value)
                sku, price, stock, description = tuple(values)
            else:
                sku, price, stock, description = tuple([None] * 4)
                logging.warning("cannot found JSON {}".format(link))

        else:  # This is April
            currency = html.css_first("p.price span[class*=currency]").text(strip=True)
            price = html.css("p.price span[class*=amount]")
            price = list(
                map(
                    lambda p: int(
                        p.text(strip=True).replace(currency, "").replace(".", "")
                    ),
                    price,
                )
            )
            if len(price) > 1:
                price = list(filter(lambda p: min(p), price))
            else:
                price = list(price)
            stock = [
                bool(
                    re.findall(
                        "in", html.css_first("[class*=stock]").attributes.get("class")
                    )
                )
            ] * len(price)
            sku = [self.get_sku(item)] * len(price)  # sku available in url
            description = [item.get("excerpt").get("rendered")] * len(price)

        return price, stock, sku, description

    def generate_dataset(self):
        # date_fmt = "%Y-%m-%dT%H:%M:%S%z"
        tzinfo = timezone(timedelta(hours=7))
        date_acquisition = datetime.now(tzinfo).replace(microsecond=0).isoformat()

        if self.rawdata:
            # images = list(map(lambda item: item.get("images"), self.rawdata))
            # tag = list(map(lambda item: item.get("tags"), self.rawdata))
            # variants = list(map(lambda item: item.get("variants"), self.rawdata))
            product_columns = [
                "product_id",
                "sku",
                "name",
                "brand",
                "category",
                "variant_id",
                "variant_name",
                "date_release",
                "description",
                "slug",
            ]
            offers_columns = [
                "product_id",
                "variant_id",
                "sku",
                "price",
                "is_instock",
                "date_acquisition",
                "source",
            ]
            product_collections, offers_collections = [], []
            for item in self.rawdata:
                sleep(randrange(0, 2))
                price, stock, sku, description = self.get_offers(item)
                if not description:
                    description = [item.get("excerpt").get("rendered")] * len(sku)

                product_content = zip(
                    [str(item.get("id"))] * len(sku),
                    sku,
                    [self.get_clean_name(item)] * len(sku),
                    [self.define_brand(item)] * len(sku),
                    [self.get_category(item)] * len(sku),
                    [None] * len(sku),
                    [None] * len(sku),
                    ["{}+07:00".format(item.get("date"))] * len(sku),
                    description,
                    [item.get("slug")] * len(sku),
                )
                product = pd.DataFrame(list(product_content), columns=product_columns)
                product_collections.append(product)

                offers_content = zip(
                    [str(item.get("id"))] * len(sku),
                    [None] * len(sku),
                    sku,
                    price,
                    stock,
                    [date_acquisition] * len(sku),
                    [self.get_website(item)] * len(sku),
                )
                offers = pd.DataFrame(list(offers_content), columns=offers_columns)
                offers_collections.append(offers)

            products = pd.concat(product_collections, ignore_index=True)
            offers = pd.concat(offers_collections, ignore_index=True)
            return products, offers
        else:
            logging.info("empty feed, consider raw data to Transformer")
            return
