#database = input("Введите путь к базе данных: ")
#txt_path = input("ВВЕДИТЕ путь к папке с файлами txt/xz: ")

import csv
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
import lzma
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
database = "/home/taruu/PycharmProjects/tus_database_lib/database_work.db"
txt_path = "/home/taruu/PycharmProjects/tus_database_lib/test_txt"

class File_work:
    def __init__(self,database_path:str):
        self.data_event = DataEvents(database_path)

    def load_file_xz(self,filename):
        "Load from xz file"
        r_filename = filename.split("/")[-1]
        start = r_filename.split("-")[1]
        end = r_filename.split("-")[2]
        startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
        enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()
        frames_x16 = []  # массив по 16*16 *256
        lines = []
        with lzma.open(filename, "rt") as inp:
            for line in list(islice(inp, 2560)):
                l = [x for x in (' '.join(line.split())).split(' ')]
                lines.append(l)

        with lzma.open(filename, "rt") as inp:
            list_hv = next(islice(inp, 256, 257)).split()
            list_hv.pop(0)
            list_hv.pop(0)

        with lzma.open(filename, "rt") as inp:
            LLA_coordinates = next(islice(inp, 268, 269)).split()
            LLA_coordinates.pop(0)
            LLA_coordinates.pop(0)

        for j in range(2, 258):
            frame = []
            for k in range(16):
                row = []
                for l in range(16):
                    row.append(int(lines[16 * k + l][j]))
                frame.append(row)
            frames_x16.append(frame)

        return {"frames_x16": frames_x16, "lsit_hv": list_hv, "LLA_coordinates": LLA_coordinates,
                "start": startdatatime,
                "end": enddatatime,
                "filename": r_filename}

    def load_file_txt(self,filename):
        "load from txt file"
        r_filename = filename.split("/")[-1]
        start = r_filename.split("-")[1]
        end = r_filename.split("-")[2]
        startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
        enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()
        frames_x16 = []  # массив по 16*16 *256
        lines = []
        with open(filename) as inp:
            for line in list(islice(inp, 2560)):
                l = [x for x in (' '.join(line.split())).split(' ')]
                lines.append(l)

        with open(filename) as inp:
            list_hv = next(islice(inp, 256, 257)).split()
            list_hv.pop(0)
            list_hv.pop(0)

        with open(filename) as inp:
            LLA_coordinates = next(islice(inp, 268, 269)).split()
            LLA_coordinates.pop(0)
            LLA_coordinates.pop(0)

        for j in range(2, 258):
            frame = []
            for k in range(16):
                row = []
                for l in range(16):
                    row.append(int(lines[16 * k + l][j]))
                frame.append(row)
            frames_x16.append(frame)

        return {"frames_x16": frames_x16, "lsit_hv": list_hv, "LLA_coordinates": LLA_coordinates,
                "start": startdatatime, "end": enddatatime,"filename":r_filename}

    def insert_data(self, data_dict:dict):
        #TODO СВОЙ СЧЕТЧИК ТАК КАК ТАК БЫСТРЕЕ
        event_item = datadriver.event(data_dict["filename"],
                         data_dict["start"],
                         data_dict["end"],
                         *data_dict["LLA_coordinates"]
                         )
        print("try add")

        try:
            self.data_event.session.add(event_item)
            self.data_event.session.flush()
            print("add item")
        except:
            return False
        for matrix in data_dict["frames_x16"]:
            matrix_item = datadriver.matrix(event_item.id)
            try:
                self.data_event.session.add(matrix_item)
                self.data_event.session.flush()
            except:
                return False
            for line in matrix:
                #print(line)
                self.data_event.session.add(datadriver.line(matrix_item.id,line))

        self.data_event.session.commit()

    def take_convert(self, filename):
        if filename.split(".")[-1] == "xz":
            data_all = self.load_file_xz(file)
        else:
            data_all = self.load_file_txt(file)
        return self.insert_data(data_all)



add_list = []
print(database)
print(txt_path)
list_files = glob.glob(txt_path+"/*.txt")
list_xz = glob.glob(txt_path+"/*/*")
if len(list_xz) != 0:
    list_files.extend(list_xz)


File_worker = File_work(database)

for id,file in enumerate(list_files):
    print(id,file)
    File_worker.take_convert(file)
