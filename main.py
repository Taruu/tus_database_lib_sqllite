#database = input("Введите путь к базе данных: ")
#txt_path = input("ВВЕДИТЕ путь к папке с файлами txt/xz: ")

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
import lzma
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
database = "/home/taruu/PycharmProjects/tus_database_lib_sqllite/database_work.db"
txt_path = "/home/taruu/PycharmProjects/tus_database_lib_sqllite/test_txt"

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
            hash_str = hashlib.sha224(inp).hexdigest()
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
                "filename": r_filename,
                "hash":hash_str}

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
            hash_str = hashlib.sha224(inp)
            print(hash_str)
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
                "start": startdatatime, "end": enddatatime,"filename":r_filename,"hash":hash_str}

    def insert_data(self, data_dict:dict):

        #TODO СВОЙ СЧЕТЧИК ТАК КАК ТАК БЫСТРЕЕ

        input()
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

    def data_to_database_insert_list(self,list_in_data):
        print(f"Import in databese {len(list_in_data)} items")
        obj_last_event = self.data_event.session.query(datadriver.event).order_by(datadriver.event.id.desc()).first()
        if obj_last_event:
            last_event_id = obj_last_event.id
            last_matrix_id = obj_last_event.matrixs[-1].id
            last_line_id = obj_last_event.matrixs[-1].lines[-1].id
        else:
            last_event_id = 0
            last_matrix_id = 0
            last_line_id = 0
        sql_all_add = []
        #print(last_event_id,last_matrix_id,last_line_id)
        for id,data_dict in enumerate(list_in_data):
            last_event_id += 1
            event_item = datadriver.event(last_event_id,
                                          data_dict["hash"],
                                          data_dict["filename"],
                                          data_dict["start"],
                                          data_dict["end"],
                                          *data_dict["LLA_coordinates"])

            sql_all_add.append(event_item)
            for matrix in data_dict["frames_x16"]:
                last_matrix_id += 1
                matrix_item = datadriver.matrix(last_matrix_id,event_item.id)
                sql_all_add.append(matrix_item)

                for line in matrix:
                    last_line_id += 1
                    sql_all_add.append(datadriver.line(last_line_id,matrix_item.id, line))
                    #print(last_event_id, last_matrix_id, last_line_id)
        else:
            self.data_event.session.add_all(sql_all_add)
            self.data_event.session.commit()










    def take_convert(self, filename):
        if filename.split(".")[-1] == "xz":
            data_all = self.load_file_xz(file)
        else:
            data_all = self.load_file_txt(file)

        return data_all



add_list = []
print(database)
print(txt_path)
list_files = glob.glob(txt_path+"/*.txt")
list_xz = glob.glob(txt_path+"/*/*")
if len(list_xz) != 0:
    list_files.extend(list_xz)


File_worker = File_work(database)

list_to_add = []
for id,file in enumerate(list_files):
    print(id+1,file)
    if len(list_to_add) > 10:
        File_worker.data_to_database_insert_list(list_to_add)
        list_to_add.clear()
    else:
        obj = File_worker.take_convert(file)
        if obj:
            list_to_add.append(obj)
else:
    if list_to_add:
        File_worker.data_to_database_insert_list(list_to_add)
