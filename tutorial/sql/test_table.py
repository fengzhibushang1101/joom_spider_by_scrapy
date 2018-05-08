# coding=utf8
__author__ = 'changdongsheng'
import datetime
import traceback

import sqlalchemy as SA
from tutorial.sql.base import Base, db
from tutorial.sql.session import sessionCM
# from lib.utils.logger_utils import logger
from sqlalchemy import text


class TestTable(Base):
    __tablename__ = "test"

    id = SA.Column(SA.Integer(), primary_key=True, autoincrement=True)
    name = SA.Column(SA.String(512), nullable=False)
    value = SA.Column(SA.String(512), nullable=False)

