# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from sqlalchemy import text

from tutorial.items import CategoryItem, ShopItem, ProductItem, ProductBodyItem, UserItem, JoomReviewsItem
from tutorial.nosql.mongo_utils import Coll
from tutorial.sql.base import get_session


class TutorialPipeline(object):
    def process_item(self, item, spider):
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.shop_no = set()
        self.user_no = set()

    def process_item(self, item, spider):
        if isinstance(item, ShopItem):
            if item['shop_no'] in self.shop_no:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.shop_no.add(item["shop_no"])
        elif isinstance(item, UserItem):
            if item["user_no"] in self.user_no:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.user_no.add(item["user_no"])
        return item


class CategoryPipeline(object):
    review_coll = Coll("joom_reviews")
    pro_coll = Coll("joom_pro")
    session = get_session()

    def __init__(self):
        self.session_count = 0

    def process_item(self, item, spider):
        if isinstance(item, CategoryItem):
            sql_str = text(
                "insert into category (tag, category.name, p_id, is_leaf, category.level, site_id, status) values (:tag, :name, :p_id, :is_leaf, :level, :site_id, :status) on duplicate key update status=:status")
            self.session.execute(sql_str, dict(item))
            self.session.commit()
        elif isinstance(item, JoomReviewsItem):
            self.review_coll.insert_many(item["bodys"])
        else:
            self.pro_coll.save(item["body"])
        return item
        # return item
        # if isinstance(item, CategoryItem):
        #     sql_str = text(
        #         "insert into category (tag, category.name, p_id, is_leaf, category.level, site_id, status) values (:tag, :name, :p_id, :is_leaf, :level, :site_id, :status) on duplicate key update status=:status")
        #     self.session.execute(sql_str, dict(item))
        #     self.session.commit()
        #     return item
        # elif isinstance(item, ShopItem):
        #     sql_str = text('insert into joom_shop (joom_shop.name,shop_no,logo,rate,save_count,create_time,update_time,is_verify,pro_count,reviews_count,r_count_30,r_count_7,r_count_7_14,growth_rate,cate_id) values (:name,:shop_no,:logo,:rate,:save_count,:create_time,:update_time,:is_verify,0,0,0,0,0,0,"") on duplicate key update rate=:rate, save_count=:save_count, create_time=:create_time, update_time=:update_time, is_verify=:is_verify;')
        # elif isinstance(item, ProductItem):
        #     sql_str = text('insert into joom_pro (joom_pro.name,pro_no,shop_no,category_id,image,rate,msrp,discount,real_price,reviews_count,create_time,update_time,cate_id1,cate_id2,cate_id3,cate_id4,cate_id5,origin_price,r_count_30,r_count_7,r_count_7_14,growth_rate,save_count) values (:name,:pro_no,:shop_no,:category_id,:image,:rate,:msrp,:discount,:real_price,:reviews_count,:create_time,:update_time,"","","","","",0,0,0,0,0,0) on duplicate key update joom_pro.name=:name,category_id=:category_id,rate=:rate,msrp=:msrp,discount=:discount,real_price=:real_price,reviews_count=:reviews_count,update_time=:update_time;')
        # elif isinstance(item, ProductBodyItem):
        #     sql_str = text('insert into product_body (product_body.key, cate, site, body) values (:key, :cate, :site, :body) on duplicate key update cate=:cate, body=:body')
        # elif isinstance(item, UserItem):
        #     sql_str = text('insert into joom_user (user_no, full_name, images) values (:user_no, :full_name, :images) on duplicate key update full_name=:full_name, images = :images;')
        # else:
        #     sql_str = text('insert into joom_review (review_no,create_time,update_time,pro_no,variation_id,user_no,joom_review.language,origin_text,new_text,order_id,is_anonymous,colors,star,shop_no,photos) VALUES (:review_no,:create_time,:update_time,:pro_no,:variation_id,:user_no,:language,:origin_text,:new_text,:order_id,:is_anonymous,:colors,:star,:shop_no,:photos) on duplicate key update star=:star;')
        # self.session.execute(sql_str, dict(item))
        # self.session_count += 1
        # if self.session_count > 10000:
        #     self.session_count = 0
        #     self.session.commit()
        # return item

    def close_spider(self, spider):
        self.session.commit()
        self.session.close()


if __name__ == "__main__":
    from tutorial.items import CategoryItem, ShopItem, UserItem, ProductItem

    CategoryPipeline().process_item(CategoryItem(
        tag="11111123111111",
        name=u"test1",
        p_id=123,
        is_leaf=1,
        level=44,
        site_id=31,
        status=0
    ), None)
