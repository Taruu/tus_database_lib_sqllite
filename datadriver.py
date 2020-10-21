import csv
from itertools import islice
import numpy as np
import sqlalchemy
import glob
import array
import hashlib
from sqlalchemy import Column, Integer, String,BIGINT,BLOB
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
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
        self.matrix_x256 = None

class matrix(Base):
    __tablename__ = 'matrix'
    id = Column(Integer,primary_key=True,index=True,autoincrement=True)
    event = Column(Integer,ForeignKey("event.id"))
    data = Column(BLOB)
    def __init__(self,id_in:int,event:int,data:bytes):
        self.id = id_in
        self.event = event
        self.data = data
class hv_line(Base):
    __tablename__ = 'hv_line'
    id = Column(Integer, ForeignKey("event.id"),primary_key=True, index=True,autoincrement=True)
    #event = Column(Integer,ForeignKey("event.id"))
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




# class line(Base):
#     __tablename__ = 'line'
#     id = Column(Integer,autoincrement=True, primary_key=True, index=True)
#     #TODO связь
#     matrix = Column(Integer,ForeignKey('matrix.id'))
#     data_1 = Column(Integer)
#     data_2 = Column(Integer)
#     data_3 = Column(Integer)
#     data_4 = Column(Integer)
#     data_5 = Column(Integer)
#     data_6 = Column(Integer)
#     data_7 = Column(Integer)
#     data_8 = Column(Integer)
#     data_9 = Column(Integer)
#     data_10 = Column(Integer)
#     data_11 = Column(Integer)
#     data_12 = Column(Integer)
#     data_13 = Column(Integer)
#     data_14 = Column(Integer)
#     data_15 = Column(Integer)
#     data_16 = Column(Integer)
#     def __init__(self,id_in, matrix,list_line):
#         self.id = id_in
#         self.matrix = matrix
#         self.data_1 = list_line[0]
#         self.data_2 = list_line[1]
#         self.data_3 = list_line[2]
#         self.data_4 = list_line[3]
#         self.data_5 = list_line[4]
#         self.data_6 = list_line[5]
#         self.data_7 = list_line[6]
#         self.data_8 = list_line[7]
#         self.data_9 = list_line[8]
#         self.data_10= list_line[9]
#         self.data_11= list_line[10]
#         self.data_12= list_line[11]
#         self.data_13= list_line[12]
#         self.data_14= list_line[13]
#         self.data_15= list_line[14]
#         self.data_16= list_line[15]


class DataEvents:
    def __init__(self,database):
        engine = create_engine('sqlite:///' + database,echo=False)
        #DBSession = scoped_session(sessionmaker())
        # DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
        # Base.metadata.drop_all(engine)
        # Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        self.session = Session()

    def load_file_xz(self,filename:str):
        "Load from xz file"
        r_filename = filename.split("/")[-1]
        start = r_filename.split("-")[1]
        end = r_filename.split("-")[2]
        startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
        enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()
        frames_x16 = []  # массив по 16*16 *256
        lines = []
        with lzma.open(filename, "rt") as inp:
            hash_str = hashlib.md5(inp.read().decode())
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
            hash_str = hashlib.md5(str.encode(inp.read(), encoding='utf-8')).hexdigest()
            inp.seek(0)
            for line in list(islice(inp, 2560)):
                l = [x for x in (' '.join(line.split())).split(' ')]
                lines.append(l)
            inp.seek(0)
            list_hv = next(islice(inp, 256, 257)).split()
            list_hv.pop(0)
            list_hv.pop(0)

            inp.seek(0)
            LLA_coordinates = next(islice(inp, 268, 269)).split()
            LLA_coordinates.pop(0)
            LLA_coordinates.pop(0)

       # with open(filename) as inp:


        #with open(filename) as inp:


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

    def take_convert(self, filename):
        if filename.split(".")[-1] == "xz":
            data_all = self.load_file_xz(filename)
        else:
            data_all = self.load_file_txt(filename)
        hash_file = data_all["hash"]
        obj = self.session.query(event).filter_by(hash=hash_file).first()
        if not obj:
            return data_all
        else:
            return None

    def data_to_database_insert_list(self,list_in_data):
        start = time.time()
        print(f"Import in databese {len(list_in_data)} items")
        obj_last_event = self.session.query(event).order_by(event.id.desc()).first()
        if obj_last_event:
            last_event_id = obj_last_event.id
            last_matrix_id = obj_last_event.matrixs[-1].id
            #last_line_id = obj_last_event.matrixs[-1].lines[-1].id
        else:
            last_event_id = 0
            last_matrix_id = 0
            #last_line_id = 0
        sql_all_add = []
        #print(last_event_id,last_matrix_id,last_line_id)
        for id,data_dict in enumerate(list_in_data):
            last_event_id += 1
            event_item = event(last_event_id,
                               data_dict["hash"],
                               data_dict["filename"],
                               data_dict["start"],
                               data_dict["end"],
                               *data_dict["LLA_coordinates"])
            hv_line_var = hv_line(last_event_id,data_dict["lsit_hv"])

            sql_all_add.append(event_item)
            sql_all_add.append(hv_line_var)
            # print(array.array("h",np.array(data_dict["frames_x16"]).flat))
            # print("arrays", sys.getsizeof())
            # print("text",sys.getsizeof(str(data_dict["frames_x16"])))
            # print("matrix", sys.getsizeof(pickle.dumps(data_dict["frames_x16"])))
            # print("turpule", sys.getsizeof(pickle.dumps(tuple(data_dict["frames_x16"]))))
            # print("torch", sys.getsizeof(pickle.dumps(torch.Tensor(data_dict["frames_x16"]))))
            for matrix in data_dict["frames_x16"]:
                last_matrix_id += 1
                matrix_item = matrix(last_matrix_id,event_item.id,array.array("h",np.array(matrix).flat).tobytes())
                sql_all_add.append(matrix_item)

                # for line in matrix:
                #     last_line_id += 1
                #     sql_all_add.append(datadriver.line(last_line_id,matrix_item.id, line))
                #     #print(last_event_id, last_matrix_id, last_line_id)
        else:
            self.session.add_all(sql_all_add)
            self.session.flush()
            self.session.commit()
        print(f"Data add {time.time() - start}")



    def insert_data_to_database(self,folder_txt_or_xz):
        count_all = 0
        list_files = glob.glob(folder_txt_or_xz + "/*.txt")
        list_xz = glob.glob(folder_txt_or_xz + "/*/*")
        if len(list_xz) != 0:
            list_files.extend(list_xz)


        list_to_add = []
        for id, file in enumerate(list_files):
            print(id + 1, file)
            if len(list_to_add) >= 100:
                self.data_to_database_insert_list(list_to_add)
                list_to_add.clear()
            else:
                obj = self.take_convert(file)
                if obj:
                    count_all+=1
                    list_to_add.append(obj)
        else:
            if list_to_add:
                self.data_to_database_insert_list(list_to_add)

    def all_data(self):
        obj_last_event = self.session.query(event).order_by(event.id.desc()).first()
        for id in range(1,obj_last_event.id+1):
            obj = self.session.query(event).get(id)
            matrix_x256 = None
            list_matrix_x256 = []
            for matrix in obj.matrixs:
                data = array.array('h', [])
                data.frombytes(matrix.data)
                np_matrix = np.array(data.tolist()).reshape(16, 16)
                list_matrix_x256.append(np_matrix)
            else:
                matrix_x256 = np.array(list_matrix_x256)
                list_matrix_x256.clear()
            obj.matrixs_x256 = matrix_x256
            yield obj

