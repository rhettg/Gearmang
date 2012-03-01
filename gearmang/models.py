# -*- coding: utf-8 -*-
"""
This module contains the primary sqlalchemy models that power Gearmang.

:copyright: (c) 2012 by Rhett Garber
:license: ISC, see LICENSE for more details.
"""
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index, Column, Integer, String, DateTime, LargeBinary, Text

Base = declarative_base()

class FailedGearmanJob(Base):
    __tablename__ = 'failed_gearman_job'

    id = Column(Integer, primary_key=True)
    time_created = Column(DateTime, nullable=False, default=sqlalchemy.sql.func.now())
    unique_key = Column(String(64), unique=True, nullable=False)
    task = Column(String(64), nullable=False)
    data = Column(LargeBinary)
    time_failed = Column(DateTime)
    exception = Column(Text)
