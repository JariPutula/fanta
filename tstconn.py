# -*- coding: utf-8 -*-
"""
Created on Sun Sep  7 17:10:57 2025

@author: jarip
"""

from sqlalchemy import create_engine
e = create_engine("postgresql+psycopg2://postgres:admin@localhost:5433/fantacalcio")
with e.connect() as c:
    print(c.exec_driver_sql("select version()").scalar_one())