# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TutorialItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class CategoryItem(scrapy.Item):
    tag = scrapy.Field()
    name = scrapy.Field()
    p_id = scrapy.Field()
    is_leaf = scrapy.Field()
    level = scrapy.Field()
    pin = scrapy.Field()
    site_id = scrapy.Field()
    status = scrapy.Field()


class ProductItem(scrapy.Item):
    name = scrapy.Field()
    pro_no = scrapy.Field()
    shop_no = scrapy.Field()
    category_id = scrapy.Field()
    image = scrapy.Field()
    rate = scrapy.Field()
    msrp = scrapy.Field()
    discount = scrapy.Field()
    real_price = scrapy.Field()
    reviews_count = scrapy.Field()
    # r_count_7_14 = scrapy.Field()
    # r_count_30 = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()


class ShopItem(scrapy.Item):
    name = scrapy.Field()
    shop_no = scrapy.Field()
    logo = scrapy.Field()
    rate = scrapy.Field()
    save_count = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    is_verify = scrapy.Field()


class ProductBodyItem(scrapy.Item):
    key = scrapy.Field()
    cate = scrapy.Field()
    site = scrapy.Field()
    body = scrapy.Field()


class UserItem(scrapy.Item):
    user_no = scrapy.Field()
    full_name = scrapy.Field()
    images = scrapy.Field()


class ReviewItem(scrapy.Item):
    review_no = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    pro_no = scrapy.Field()
    variation_id = scrapy.Field()
    user_no = scrapy.Field()
    language = scrapy.Field()
    origin_text = scrapy.Field()
    new_text = scrapy.Field()
    order_id = scrapy.Field()
    is_anonymous = scrapy.Field()
    colors = scrapy.Field()
    star = scrapy.Field()
    shop_no = scrapy.Field()
    photos = scrapy.Field()


class JoomProItem(scrapy.Item):
    body = scrapy.Field()


class JoomReviewsItem(scrapy.Item):
    bodys = scrapy.Field()
