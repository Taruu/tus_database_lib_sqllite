import csv
import hashlib
import  datadriver
from datadriver import DataEvents
from itertools import islice
import numpy as np
import sqlalchemy
import glob
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pickle
from datetime import datetime
import array
import lzma
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
database = "/home/taruu/PycharmProjects/tus_database_lib_sqllite/database_work.db"

data_event = DataEvents(database)
print(data_event.session.query(datadriver.event).count() )
start = time.time()
for i in range(1,data_event.session.query(datadriver.event).count()):
    obj = data_event.session.query(datadriver.event).get(i)
    for matrix in obj.matrixs:
        data = array.array('h',[])
        data.frombytes(matrix.data)
        print(obj.name)
        print(data)
        np_matrix = np.array(data.tolist()).reshape(16,16)
        print(np_matrix)
        input()
print(time.time()-start)