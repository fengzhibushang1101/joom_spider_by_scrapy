#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/8 15:23
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import datetime
import ujson as json


def trans_pro(res):
    pro_data = dict()
    item = res["payload"]
    tag = item["id"]
    shop_data = item["store"]
    shop_info = {
        "name": shop_data["name"],
        "shop_no": shop_data["id"],
        "logo": "" if not shop_data.get("image") else shop_data["image"]["images"][3]["url"],
        "rate": shop_data.get("rating", 0),
        "is_verify": "1" if shop_data["verified"] else "0",
        "save_count": shop_data["favoritesCount"]["value"],
        "create_time": datetime.datetime.fromtimestamp(shop_data[
                                                           "updatedTimeMerchantMs"] / 1000) if "updatedTimeMerchantMs" in shop_data else datetime.datetime.strptime(
            "1997-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
        "update_time": datetime.datetime.fromtimestamp(shop_data[
                                                           "updatedTimeMerchantMs"] / 1000) if "updatedTimeMerchantMs" in shop_data else datetime.datetime.strptime(
            "1997-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    }
    pro_info = {
        "name": item["name"],
        "pro_no": item["id"],
        "shop_no": item["storeId"],
        "category_id": item.get("categoryId", "0"),
        "image": item["mainImage"]["images"][3]["url"] if "mainImage" in item else "",
        "rate": item.get("rating", 0),
        "msrp": item["lite"].get("msrPrice", 0),
        "discount": item["lite"].get("discount", 0),
        "real_price": item["lite"]["price"],
        "reviews_count": item["reviewsCount"]["value"],
        "create_time": datetime.datetime.fromtimestamp(min(map(lambda z: z["createdTimeMs"], item[
            "variants"])) / 1000) if "variants" in item else datetime.datetime.strptime("1997-01-01 00:00:00",
                                                                                        "%Y-%m-%d %H:%M:%S"),
        "update_time": datetime.datetime.fromtimestamp(max(map(lambda z: z["publishedTimeMs"], item[
            "variants"])) / 1000) if "variants" in item else datetime.datetime.strptime("1997-01-01 00:00:00",
                                                                                        "%Y-%m-%d %H:%M:%S")  # 这里不准确
    }
    # pro_info["r_count_30"] = pro_info["r_count_7"] = pro_info["r_count_7_14"] = pro_info["growth_rate"] = 0
    parent_info = item["lite"]
    pro_data["SourceInfo"] = {
        "Platform": "Joom",
        "Link": "https://www.joom.com/en/products/%s/" % tag,
        "Site": "Global",
        "SiteID": 31,
        "ProductID": tag
    }
    pro_data["Title"] = item["name"]
    pro_data["rating"] = item.get("rating", 0)
    pro_data["reviews_count"] = item["reviewsCount"]
    pro_data["store_id"] = item["storeId"]
    pro_data["keywords"] = item.get("tags", [])
    pro_data["price"] = parent_info["price"]
    pro_data["MSRP"] = parent_info.get("msrPrice", 0)
    pro_data["discount"] = parent_info["discount"]
    pro_data["Description"] = item["description"]
    pro_data["ProductSKUs"] = list()
    pro_data["images"] = get_images(item)
    pro_data["ProductSKUs"] = get_variants(item["variants"]) if "variants" in item else []
    pro_body = {"key": item["id"], "site": 31, "body": json.dumps(pro_data), "cate": item.get("categoryId", "0")}
    return pro_body, shop_info, pro_info


def get_images(item):
    try:
        extra_images = [image["payload"]["images"][3]["url"] for image in item["gallery"] if image["type"] == "image"]
    except:
        extra_images = []
    main_image = item["mainImage"]["images"][3]["url"] if "mainImage" in item else ""
    return [main_image] + extra_images


def get_variants(variations):
    pro_vars = []
    for variation in variations:
        v_specifics = []
        if variation.get("colors"):
            v_specifics.append({
                "Image": [],
                "ValueID": variation["colors"][0].get("rgb", ""),
                "NameID": "",
                "Name": "Color",
                "Value": variation["colors"][0]["name"]
            })
        if variation.get("size"):
            v_specifics.append({
                "Image": [],
                "ValueID": "",
                "NameID": "",
                "Name": "Size",
                "Value": variation["size"]
            })
        pro_vars.append({
            "SkuID": variation["id"],  # 变体SKUID
            "PictureURL": "" if not variation.get("mainImage") else variation["mainImage"]["images"][3]["url"],
            "Active": variation["inStock"],
            "VariationSpecifics": v_specifics,  # 变体属性信息
            "Price": variation["price"],  # 价格
            "ShippingTime": "%s-%s" % (variation["shipping"]["maxDays"], variation["shipping"]["minDays"]),  # 运送时间
            "ShippingCost": variation["shipping"]["price"],  # 运费
            "MSRP": variation.get("msrPrice", 0),  # msrp
            "Stock": variation["inventory"]  # 库存a
        })
    return pro_vars
