#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/8 16:12
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import datetime

import ujson as json

def retrieve_review(reviews_info):
    try:
        review_users = []
        review_datas = []
        for review in reviews_info:
            review_data = {
                "review_no": review["id"],
                "create_time": datetime.datetime.fromtimestamp(review["createdTimeMs"] / 1000),
                "update_time": datetime.datetime.fromtimestamp(review["updatedTimeMs"] / 1000),
                "pro_no": review["productId"],
                "variation_id": review["productVariantId"],
                "user_no": review["user"]["id"] if review.get("user") else "",
                "language": review.get("originalLanguage", ""),
                "origin_text": review.get("originalText", ""),
                "new_text": review.get("text", ""),
                "order_id": review.get("orderId", ""),
                "is_anonymous": review["isAnonymous"],
                "colors": json.dumps(review["productVariant"]["colors"]) if review["productVariant"].get(
                    "colors") else "",
                "star": review["starRating"],
                "shop_no": review["productLite"]["storeId"],
                "photos": ""
            }
            if review.get("user"):
                user_data = {
                    "full_name": review["user"].get("fullName", ""),
                    "user_no": review["user"]["id"],
                    "images": review["user"]["avatar"]["images"][0]["url"] if review["user"].get("avatar") else ""
                }
                review_users.append(user_data)
            review_datas.append(review_data)
        return review_datas, review_users
    except Exception as e:
        raise Exception("reviewer error")