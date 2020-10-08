import csv
from itertools import islice
import numpy as np
import sqlalchemy
import glob
from sqlalchemy import Column, Integer, String,BIGINT
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pickle
from datetime import datetime
import lzma
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim



Base = declarative_base()


class event(Base):
    __tablename__ = 'event'
    id = Column(Integer, primary_key=True, index=True,autoincrement=True)
    hash = Column(String,unique=True)
    name = Column(String,unique=True)
    start = Column(BIGINT,index=True)
    end = Column(BIGINT)
    lat = Column(Integer,index=True)
    lon = Column(Integer,index=True)
    height = Column(Integer,index=True)
    matrixs = relationship("matrix")
    def __init__(self,id_in,hash, name, start, end, lat, lon, height):
        self.id = id_in
        self.hash = hash
        self.name = name
        self.start = int(start)
        self.end = int(end)
        self.lat = float(lat)
        self.lon = float(lon)
        self.height = float(height)

class matrix(Base):
    __tablename__ = 'matrix'
    id = Column(Integer,primary_key=True,index=True,autoincrement=True)
    event = Column(Integer,ForeignKey("event.id"))
    lines = relationship("line")
    def __init__(self,id_in:int,event:int):
        self.id = id_in
        self.event = event

class line(Base):
    __tablename__ = 'line'
    id = Column(Integer,autoincrement=True, primary_key=True, index=True)
    #TODO связь
    matrix = Column(Integer,ForeignKey('matrix.id'))
    data_1 = Column(Integer)
    data_2 = Column(Integer)
    data_3 = Column(Integer)
    data_4 = Column(Integer)
    data_5 = Column(Integer)
    data_6 = Column(Integer)
    data_7 = Column(Integer)
    data_8 = Column(Integer)
    data_9 = Column(Integer)
    data_10 = Column(Integer)
    data_11 = Column(Integer)
    data_12 = Column(Integer)
    data_13 = Column(Integer)
    data_14 = Column(Integer)
    data_15 = Column(Integer)
    data_16 = Column(Integer)
    def __init__(self,id_in, matrix,list_line):
        self.id = id_in
        self.matrix = matrix
        self.data_1 = list_line[0]
        self.data_2 = list_line[1]
        self.data_3 = list_line[2]
        self.data_4 = list_line[3]
        self.data_5 = list_line[4]
        self.data_6 = list_line[5]
        self.data_7 = list_line[6]
        self.data_8 = list_line[7]
        self.data_9 = list_line[8]
        self.data_10= list_line[9]
        self.data_11= list_line[10]
        self.data_12= list_line[11]
        self.data_13= list_line[12]
        self.data_14= list_line[13]
        self.data_15= list_line[14]
        self.data_16= list_line[15]

class hv_line(Base):
    __tablename__ = 'hv_line'
    id = Column(Integer, primary_key=True, index=True,autoincrement=True)
    event = Column(Integer,ForeignKey("event.id"))
    hv1 = Column(Integer, index=True)
    hv2 = Column(Integer, index=True)
    hv3 = Column(Integer, index=True)
    hv4 = Column(Integer, index=True)
    hv5 = Column(Integer, index=True)
    hv6 = Column(Integer, index=True)
    hv7 = Column(Integer, index=True)
    hv8 = Column(Integer, index=True)
    hv9 = Column(Integer, index=True)
    hv10 = Column(Integer, index=True)
    hv11 = Column(Integer, index=True)
    hv12 = Column(Integer, index=True)
    hv13 = Column(Integer, index=True)
    hv14 = Column(Integer, index=True)
    hv15 = Column(Integer, index=True)
    hv16 = Column(Integer, index=True)
    hv17 = Column(Integer, index=True)
    hv18 = Column(Integer, index=True)
    hv19 = Column(Integer, index=True)
    hv20 = Column(Integer, index=True)
    hv21 = Column(Integer, index=True)
    hv22 = Column(Integer, index=True)
    hv23 = Column(Integer, index=True)
    hv24 = Column(Integer, index=True)
    hv25 = Column(Integer, index=True)
    hv26 = Column(Integer, index=True)
    hv27 = Column(Integer, index=True)
    hv28 = Column(Integer, index=True)
    hv29 = Column(Integer, index=True)
    hv30 = Column(Integer, index=True)
    hv31 = Column(Integer, index=True)
    hv32 = Column(Integer, index=True)
    def __init__(self,id_in,list_in):
        self.id = id_in
        self.hv1 = list_in[0]
        self.hv2 = list_in[1]
        self.hv3 = list_in[2]
        self.hv4 = list_in[3]
        self.hv5 = list_in[4]
        self.hv6 = list_in[5]
        self.hv7 = list_in[6]
        self.hv8 = list_in[7]
        self.hv9 = list_in[8]
        self.hv10 = list_in[9]
        self.hv11 = list_in[10]
        self.hv12 = list_in[11]
        self.hv13 = list_in[12]
        self.hv14 = list_in[13]
        self.hv15 = list_in[14]
        self.hv16 = list_in[15]
        self.hv17 = list_in[16]
        self.hv18 = list_in[17]
        self.hv19 = list_in[18]
        self.hv20 = list_in[19]
        self.hv21 = list_in[20]
        self.hv22 = list_in[21]
        self.hv23 = list_in[22]
        self.hv24 = list_in[23]
        self.hv25 = list_in[24]
        self.hv26 = list_in[25]
        self.hv27 = list_in[26]
        self.hv28 = list_in[27]
        self.hv29 = list_in[28]
        self.hv30 = list_in[29]
        self.hv31 = list_in[30]
        self.hv32 = list_in[31]


class DataEvents:
    def __init__(self,database):
        engine = create_engine('sqlite:///' + database, fast_executemany=True,echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()

