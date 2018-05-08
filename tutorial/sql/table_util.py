#coding=utf8
__author__ = 'changdongsheng'
from tutorial.sql.base import db, metadata
from tutorial.sql.category import Category
from tutorial.sql.joom_pro import JoomPro
from tutorial.sql.joom_review import JoomReview
from tutorial.sql.joom_shop import JoomShop
from tutorial.sql.joom_user import JoomUser
from tutorial.sql.product_body import ProductBody
from tutorial.sql.task_schedule import TaskSchedule
from tutorial.sql.test_table import TestTable


def create_all_tables():
    """
    创建所有表
    """
    metadata.create_all(bind=db)


if __name__ == "__main__":
    create_all_tables()