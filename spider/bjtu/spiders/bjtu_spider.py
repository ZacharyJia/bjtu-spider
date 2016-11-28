# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from bs4 import BeautifulSoup
from bjtu.items import BjtuItem


class BjtuSpider(scrapy.Spider):
    name = "bjtu_spider"
    allowed_domains = ["bjtu.edu.cn"]
    start_urls = ['http://www.bjtu.edu.cn/']

    def parse(self, response):
        item = BjtuItem()

        raw_urls = response.xpath("//a/@href").extract()
        for url in raw_urls:
            yield Request(response.urljoin(url))

        bs = BeautifulSoup(response.text)
        item['url'] = response.url
        item['content'] = bs.text.replace("\n", "")
        item['title'] = response.css("title::text").extract_first()
        yield item
