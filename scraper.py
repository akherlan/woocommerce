import httpx
from selectolax.parser import HTMLParser
import pandas as pd
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta, timezone
from random import randint
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


class WordpressScraper:
    def __init__(self, website, **kwargs):
        self.session = httpx.Client(headers=DEFAULT_HEADERS, timeout=45, **kwargs)
        self.website = website
        self.rawdata = None

    def fetch(self, limit=None):
        # read more: https://developer.wordpress.org/rest-api/
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


class Transformer:
    def __init__(self, rawdata, **kwargs):
        self.session = httpx.Client(headers=DEFAULT_HEADERS, timeout=45, **kwargs)
        self.rawdata = list(
            filter(lambda item: item.get("status") == "publish", rawdata)
        )
        self.website = self.get_website()

    def import_rawdata(self, rawdata):
        self.rawdata = rawdata

    def brand_list(self):
        return {
            "thisisapril.com": "This is April",
            "jennaandkaia.co.id": "Jenna & Kaia",
            "www.shafira.com": "Shafira",
            "elizabeth.co.id": "Elizabeth",
        }

    def define_brand(self):
        domain = urlparse(self.website).netloc
        brand = self.brand_list().get(domain)
        if brand:
            return brand
        else:
            return

    def get_website(self):
        if self.rawdata:
            domain = urlparse(self.rawdata[0].get("link"))
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
        sleep(randint(0, 2))
        response = self.session.get(url)
        if response.status_code == 200:
            logging.info(
                "GET category for {}".format(item.get("title").get("rendered"))
            )
            return ", ".join([cat.get("name").title() for cat in response.json()])
        else:
            logging.warning("GET category failed {}".format(item.get("link")))
            return

    def get_offers(self, item):
        link = item.get("link")
        response = self.session.get(link)
        if response.status_code != 200:
            logging.error("bad response {} {}".format(response.status_code), link)
            return
        logging.info("GET price {}".format(link))
        sleep(randint(0, 2))
        html = HTMLParser(response.text)
        brand_list = list(self.brand_list().values())
        brand_list.remove("This is April")
        if self.define_brand() in brand_list:
            varobj = html.css_first("form[enctype='multipart/form-data']")
            varobj = varobj.attributes.get("data-product_variations")
            varobj = json.loads(varobj)
            # print(varobj)
            sku = list(map(lambda x: x.get("sku"), varobj))
            price = list(map(lambda x: x.get("display_price"), varobj))
            stock = list(map(lambda x: x.get("is_in_stock"), varobj))
            description = list(map(lambda x: x.get("variation_description"), varobj))

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
        brand = self.define_brand()
        if brand:
            logging.info("brand: {}".format(brand))
        else:
            logging.warning("brand not included")
        # date_fmt = "%Y-%m-%dT%H:%M:%S%z"
        tzinfo = timezone(timedelta(hours=7))
        date_acquisition = datetime.now(tzinfo).replace(microsecond=0).isoformat()

        if brand in list(self.brand_list().values()):
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
                price, stock, sku, description = self.get_offers(item)
                if not len(list(filter(lambda i: bool(len(i)), description))):
                    description = [item.get("excerpt").get("rendered")] * len(sku)

                product_content = zip(
                    [str(item.get("id"))] * len(sku),
                    sku,
                    [self.get_clean_name(item)] * len(sku),
                    [brand] * len(sku),
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
                    [self.website] * len(sku),
                )
                offers = pd.DataFrame(list(offers_content), columns=offers_columns)
                offers_collections.append(offers)

            products = pd.concat(product_collections, ignore_index=True)
            offers = pd.concat(offers_collections, ignore_index=True)
            return products, offers
        else:
            return
