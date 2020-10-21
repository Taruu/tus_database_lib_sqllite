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
for item in data_event.all_data():
    print(item.matrixs_x256)
print(time.time()-start)