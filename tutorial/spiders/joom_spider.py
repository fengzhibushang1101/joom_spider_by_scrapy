#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/8 9:24
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
from urllib import urlencode

import scrapy

import ujson as json

from tutorial.items import CategoryItem, ProductBodyItem, ShopItem, ProductItem, ReviewItem, UserItem, JoomReviewsItem, \
    JoomProItem
from tutorial.nosql.mongo_utils import Coll
from tutorial.spiders.utils.execute_product import trans_pro
from tutorial.spiders.utils.execute_review import retrieve_review
from tutorial.spiders.utils.func import get_joom_token, random_key
from tutorial.sql.base import sessionCM
from tutorial.sql.category import Category


class JoomSpider(scrapy.Spider):
    name = "joom"
    allowed_domains = ["joom.com"]
    sub_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&categoryId=%s&parentLevels=1&language=en-US&currency=USD&_=jfrx%s"
    root_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&language=en-US&currency=USD&_=jfrx%s"
    batch_url = "https://api.joom.com/1.1/search/products?language=en-US&currency=USD&_=jfs3%s"
    product_url = "https://api.joom.com/1.1/products/%s?language=en-US&currency=USD&_=jfs7%s"
    headers = {"authorization": get_joom_token(), "origin": "https://www.joom.com", "content-type": "application/json"}
    review_url = "https://api.joom.com/1.1/products/%s/reviews?filter_id=all&count=%s&sort=top&language=en-US&currency=USD&_=jfs8%s"
    review_nurl = "https://api.joom.com/1.1/products/%s/reviews?filter_id=all&count=%s&sort=top&pageToken=%s&language=en-US&currency=USD&_=jfs8%s"
    review_coll = Coll("joom_reviews")
    pro_coll = Coll("joom_pro")
    cate_num = 0
    pro_num = 0
    pro_page_num = 0
    review_num = 0
    pro_no_set = set()

    def start_requests(self):
        yield self.yield_cate_task(self.root_url % random_key(4), dict(p_tag=None, level=1, p_id=0))

    def yield_cate_task(self, url, meta):
        return scrapy.FormRequest(
            url=url,
            headers=self.headers,
            meta=meta,
            formdata={},
            callback=self.parse_cate
        )

    def yield_cate_pro_task(self, cate_tag, meta):
        count = 48
        data = {
            "count": count,
            "filters": [
                {
                    "id": "categoryId",
                    "value": {
                        "type": "categories",
                        "items": [
                            {
                                "id": cate_tag
                            }
                        ]
                    }
                }
            ]
        }
        meta["cate_tag"] = cate_tag
        if meta.get("page_token"):
            data["pageToken"] = meta["page_token"]
        url = self.batch_url % random_key(4)
        return scrapy.Request(
            url=url,
            method="POST",
            headers=self.headers,
            body=json.dumps(data),
            meta=meta,
            callback=self.parse_cate_pro,
            errback=self.errback
        )

    def yield_pro_detail_task(self, pro_id):
        url = self.product_url % (pro_id, random_key(4))
        return scrapy.Request(
            url=url,
            headers=self.headers,
            callback=self.parse_pro_detail,
            errback=self.errback,
            meta={"pro_id": pro_id}
        )

    def yield_review_task(self, meta):
        rev_cnt = 100
        page_token = meta.get("page_token")
        if page_token:
            url = self.review_nurl % (meta["pro_id"], rev_cnt, page_token, random_key(4))
        else:
            url = self.review_url % (meta["pro_id"], rev_cnt, random_key(4))
        return scrapy.Request(
            url=url,
            headers=self.headers,
            callback=self.parse_reviews,
            errback=self.errback,
            meta=meta
        )

    def parse_reviews(self, response):
        if "unauthorized" in response.body:
            auth = get_joom_token()
            self.headers = {"authorization": auth, "origin": "https://www.joom.com"}
            yield self.yield_review_task(response.meta)
        self.review_num += 1
        print u"已经采集%s页评论" % self.review_num
        meta = response.meta
        content = json.loads(response.body)
        reviews = content["payload"]["items"]
        # review_datas, review_users = retrieve_review(reviews)
        if reviews:
            # yield JoomReviewsItem(bodys=reviews)
            self.review_coll.insert_many(reviews)
        if content["payload"].get("nextPageToken"):
            if len(reviews) > 0:
                meta["page_token"] = content["payload"]["nextPageToken"]
                yield self.yield_review_task(response.meta)
        # for review in review_datas:
        #     yield ReviewItem(review)
        # for user in review_users:
        #     yield UserItem(user)

    def parse_pro_detail(self, response):
        if "unauthorized" in response.body:
            auth = get_joom_token()
            self.headers = {"authorization": auth, "origin": "https://www.joom.com"}
            yield self.yield_pro_detail_task(response.meta["pro_id"])
        self.pro_num += 1
        print u"已经采集%s个产品" % self.pro_num
        content = json.loads(response.body)
        # yield JoomProItem(body=content.get("payload", {}))
        self.pro_coll.save(content.get("payload", {}))
        # pro_body, shop_info, pro_info = trans_pro(content)
        # yield ProductBodyItem(pro_body)
        # yield ShopItem(shop_info)
        # yield ProductItem(pro_info)

    def errback(self, failure):
        pass

    def parse_cate_pro(self, response):
        content = json.loads(response.body)
        meta = response.meta
        self.pro_page_num += 1
        print u"已经采集%s页分类产品" % self.pro_page_num
        if "payload" in content and "nextPageToken" in content["payload"]:
            items = content["payload"]["items"]
            for item in items:
                if item["id"] not in self.pro_no_set:
                    self.pro_no_set.add(item["id"])
                    yield self.yield_pro_detail_task(item["id"])
                    yield self.yield_review_task({"pro_id": item["id"]})
            if len(items) == 0:
                pass
            else:
                meta["page_token"] = content["payload"]["nextPageToken"]
                yield self.yield_cate_pro_task(meta["cate_tag"], meta)

    def parse_cate(self, response):
        if "unauthorized" in response.body:
            auth = get_joom_token()
            self.headers = {"authorization": auth, "origin": "https://www.joom.com"}
            yield self.yield_cate_task(response.url, response.meta)
        self.cate_num += 1
        print u"已经采集%s个分类" % self.cate_num
        meta = response.meta
        n_level = meta["level"] + 1
        content = json.loads(response.body)
        c_infos = content["payload"].get("children", [])
        with sessionCM() as session:
            for c_info in c_infos:
                tag = c_info['id']
                name = c_info['name']
                is_leaf = 0 if c_info["hasPublicChildren"] else 1
                cate = Category.find_by_site_tag(session, 31, tag)
                if not is_leaf:
                    if not cate:
                        n_p_id = Category.save(session, tag, name, meta["p_id"], is_leaf, meta["level"], 31)
                    else:
                        n_p_id = cate.id
                    yield self.yield_cate_task(self.sub_url % (tag, random_key(4)),
                                               dict(p_tag=tag, level=n_level, p_id=n_p_id))
                else:
                    cate_item = CategoryItem(
                        tag=tag,
                        name=name,
                        p_id=meta["p_id"],
                        is_leaf=is_leaf,
                        level=meta["level"],
                        site_id=31,
                        status=0
                    )
                    yield cate_item
                    yield self.yield_cate_pro_task(tag, {})
