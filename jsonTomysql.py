# from __future__ import absolute_import
# from __future__ import division
# from __future__ import print_function
import sys
import os
from matplotlib.cbook import print_cycles
import pymysql
import json
from Config import ConfigAdaptor
import math
import numpy as np
import pandas as pd

TypeDic = {1: "crack", 2: "cornerfracture", 3: "seambroken", 4: "patch", 5: "repair",
           6: "slab", 7: "light", 8: "track"}


def GPStoMercator(lat, lng):
    # GPS坐标转换为Mercator坐标
    mer_x = lng * 20037508.3427892 / 180
    mer_y = math.log(math.tan((90 + lat) * math.pi / 360)) / (math.pi / 180)
    mer_y = mer_y * 20037508.3427892 / 180
    return mer_x, mer_y


def MercatortoGPS(mer_x, mer_y):
    # mercator坐标转为GPS坐标
    lng = mer_x / 20037508.3427892 * 180
    lat = mer_y / 20037508.3427892 * 180
    lat = 180 / math.pi * (2 * math.atan(math.exp(lat * math.pi / 180)) - math.pi / 2)
    return lat, lng


class RecordPath:
    def __init__(self, strPrjDir, strInputDir):
        # import pdb;pdb.set_trace()
        self.inInputDir = strInputDir
        if strInputDir.find("\\") >= 0:
            self.Task = self.inInputDir.split("\\")[-2]
            self.TaskID = self.Task.split("_")[0]
        else:
            self.Task = self.inInputDir.split("/")[-2]
            self.TaskID = self.Task.split("_")[0]
            self.TaskLR = self.Task.split("_")[-3]
        self.inCamDir = self.inInputDir + "camera/raw3D/"

        # self.inXmlPath = strPrjDir + "image/xml/" + self.TaskID + ".xml" # 之前的xml是拼图时候生成的
        # self.inXmlPath = strInputDir + "camera/" + self.Task + "_bk" +".xml"
        self.inXmlPath = strInputDir + "camera/" + self.Task + ".xml"  # 现在使用采集时保存的xml


class FilePath:
    def __init__(self, strPrjDir, strTask, dtype):

        self.inPrjDir = strPrjDir
        self.insPrjTaskList = strTask
        if self.inPrjDir.find("\\") >= 0:
            self.iPrjID = int(strPrjDir.split("\\")[-2])
        else:
            self.iPrjID = int(strPrjDir.split("/")[-2])

        taskList = strTask.split(",")
        self.inRecordList = []
        for each in taskList:
            if each == "":
                break
            record = RecordPath(strPrjDir, each)
            self.inRecordList.append(record)

        if dtype == '3D':
            self.outSeg_Dir = self.inPrjDir + "image/disease/3D/"  # json 文件存储-3D
        elif dtype == "2Dseg":
            self.outSeg_Dir = self.inPrjDir + "image/disease/segment/"  # json 文件存储-2D分割
        elif dtype == "2Ddet":
            self.outSeg_Dir = self.inPrjDir + "image/disease/object/"  # json 文件存储 - 2D检测

    def loadSegmentationResult(self):
        disFiles = []
        disInfoList = []
        for each in self.inRecordList:
            jsonDir = self.outSeg_Dir + str(0) + "/" + each.TaskID + "/"
            disFiles += os.listdir(jsonDir)
            disFiles = [each for each in disFiles if each.find(".json") >= 0]
            for each in disFiles:
                with open(jsonDir + each, "r") as load_f:
                    info = json.load(load_f)
                    disInfoList.append(info)
        fileDict = dict(list(zip(disFiles, disInfoList)))
        # fileDict = sorted(fileDict.items(), key=lambda item: item[1]["LocalY"])
        return fileDict


class MYSQL(object):

    def __init__(self, host, user, passwd, port, database):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.database = database
        self.db = self.connect_db()
        self.cursor = self.db.cursor()

    def connect_db(self):
        return pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.passwd,
                               port=self.port,
                               database=self.database)

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("execute wrong ", e)

    def qurey(self, sql):
        try:
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            return data
        except Exception as e:
            print('qurey wrong ', e)

    def close_db(self):
        self.cursor.close()
        self.db.close()


mysql = MYSQL(host='11.0.0.64', user='root', passwd='Guimu@2022', port=3306, database='gm_das_data_db')


def delete_database(projectID, mode, type):
    # DELETE from surf_disease where project_id = 100614
    delete = '''delete from surf_disease where project_id = {} and imode = {} and type = '{}' '''.format(projectID,
                                                                                                         mode, type)

    mysql.execute(delete)


def esTomysql(strPrjDir, strInPath, iMode, dtype):
    cfg = ConfigAdaptor(iMode)

    # 采样
    sample = True

    if dtype == "2Ddet":
        sample = False

    # print("strPrjDir:", strPrjDir)
    # print("strTask:", strInPath)
    fileMgr = FilePath(strPrjDir, strInPath, dtype)
    print("FilePath init success!")

    for eachRecord in fileMgr.inRecordList:
        processedFile = fileMgr.outSeg_Dir + str(iMode) + "/" + eachRecord.TaskID + "/" + "processed.txt"
        TaskID = eachRecord.TaskID

        if True:
            # if os.path.isfile(processedFile):
            # print("already detect task: %s" % eachRecord)

            print("Load results", TaskID)
            jsfilespath = fileMgr.outSeg_Dir + \
                          str(iMode) + "/" + eachRecord.TaskID + "/"

            list_js_files = os.listdir(jsfilespath)
            n_len_js_list = len(list_js_files)
            i = 0

            for jsfile in list_js_files:
                print(f'upload To mysql -{TaskID} {i} / {n_len_js_list}')
                i += 1
                if jsfile == 'processed.txt':
                    continue

                with open(jsfilespath + jsfile) as load_file:
                    Disinfo = json.load(load_file)

                lasList = []
                ProjectID = Disinfo['projectID']
                name = Disinfo['name']
                class_name = TypeDic[Disinfo['m_AIDisType']]
                if class_name == 'slab':
                    continue
                area = Disinfo["area"]

                centlatitude, centlongitude = GPStoMercator(Disinfo['centerPoint']['latitude'],
                                                            Disinfo['centerPoint']['longitude'])

                # 把病害信息保存在MYSQL数据库里
                pstep = 8
                imgD_polygon = 'POLYGON(('
                for ll in Disinfo['lassoReal']:
                    if pstep != 8 and sample:
                        pstep += 1
                        continue
                    pstep = 1
                    lasList.append({"latitude": ll['latitude'], "longitude": ll['longitude']})
                    x, y = GPStoMercator(ll['latitude'], ll['longitude'])
                    imgD_polygon = imgD_polygon + str(x) + " " + str(y) + ","

                fx, fy = GPStoMercator(Disinfo['lassoReal'][0]['latitude'], Disinfo['lassoReal'][0]['longitude'])
                imgD_polygon = imgD_polygon + str(fx) + " " + str(fy) + '))'

                insert = '''insert into surf_disease(project_id, type, imode, name, label, geo_first_point, geo_polygon, lasso_list,area) values({},'{}',{},'{}','{}',PointFromText('POINT({} {})'), ST_GeometryFromText('{}', 3857),'{}','{}')'''.format(
                    ProjectID, dtype, iMode, name, class_name, centlatitude, centlongitude, imgD_polygon,
                    json.dumps(lasList), area)

                mysql.execute(insert)
                # print(insert)
                # sys.exit()


if __name__ == '__main__':
    # strPrjDir = "E:\\detectData\\processed\\11627\\"

    # strPrjDir = "/media/dataRep2/processed/100614/"
    # strInPath = '/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11213_2021_12_01_00_01_17/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11214_2021_12_01_00_23_57/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11212_2021_11_30_23_41_30/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11215_2021_12_01_02_38_55/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11209_2021_11_30_22_21_58/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11216_2021_12_01_03_28_50/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11208_2021_11_30_21_39_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11210_2021_11_30_22_44_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11211_2021_11_30_23_22_25/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11205_2021_11_29_02_02_30/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11207_2021_11_29_05_03_16/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11206_2021_11_29_03_11_18/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11201_2021_11_28_19_35_18/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11203_2021_11_29_00_18_41/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11199_2021_11_27_23_50_19/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11198_2021_11_27_21_52_04/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11200_2021_11_28_03_12_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11196_2021_11_27_03_21_20/,'

    # strPrjDir = "/media/dataRep2/processed/100413/"
    # strInpath = '/media/dataRep1/RawSource/2021detectdata/双流机场E7/11383_2021_07_19_03_40_29/,/media/dataRep1/RawSource/2021detectdata/双流机场E7/11382_2021_07_19_03_02_38/'

    # strPrjDir = "/media/dataRep2/processed/100777/"
    # strInpath = "/media/dataRep1/RawSource/2022detectdata/铜仁公路检测/车载3D相机/12765_2022_03_09_15_48_59_GMS_01_001_10036_SICK_3D/,/media/dataRep1/RawSource/2022detectdata/铜仁公路检测/车载3D相机/12765_2022_03_09_15_48_59_GMS_01_001_10034_SICK_3D/,/media/dataRep1/RawSource/2022detectdata/铜仁公路检测/车载3D相机/12755_2022_03_08_14_15_07_GMS_01_001_10034_SICK_3D/,/media/dataRep1/RawSource/2022detectdata/铜仁公路检测/车载3D相机/12755_2022_03_08_14_15_07_GMS_01_001_10036_SICK_3D/,"

    # strPrjDir = "/media/dataRep2/processed/100449/"
    # strInpath = "/media/dataRep1/RawSource/2021detectdata/广西高速/11689_2021_08_11_17_09_08/,/media/dataRep1/RawSource/2021detectdata/广西高速/11664_2021_08_10_15_45_27/,/media/dataRep1/RawSource/2021detectdata/广西高速/11679_2021_08_11_10_58_54/,/media/dataRep1/RawSource/2021detectdata/广西高速/11687_2021_08_11_16_48_16/,/media/dataRep1/RawSource/2021detectdata/广西高速/11676_2021_08_11_09_39_16/,/media/dataRep1/RawSource/2021detectdata/广西高速/11669_2021_08_10_17_37_02/,/media/dataRep1/RawSource/2021detectdata/广西高速/11678_2021_08_11_10_31_44/,/media/dataRep1/RawSource/2021detectdata/广西高速/11670_2021_08_11_07_24_11/,/media/dataRep1/RawSource/2021detectdata/广西高速/11684_2021_08_11_15_50_47/,/media/dataRep1/RawSource/2021detectdata/广西高速/11685_2021_08_11_16_02_00/,/media/dataRep1/RawSource/2021detectdata/广西高速/11680_2021_08_11_11_57_37/,/media/dataRep1/RawSource/2021detectdata/广西高速/11665_2021_08_10_16_51_30/,/media/dataRep1/RawSource/2021detectdata/广西高速/11677_2021_08_11_10_25_23/,/media/dataRep1/RawSource/2021detectdata/广西高速/11666_2021_08_10_17_06_51/,/media/dataRep1/RawSource/2021detectdata/广西高速/11686_2021_08_11_16_11_58/,"

    # strPrjDir = "/media/dataRep2/processed/100961/"
    # prjID = 100961
    # strInPath = "/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12527_2022_06_22_00_16_43/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12525_2022_06_21_21_13_02/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12528_2022_06_22_01_35_47/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12524_2022_06_21_19_26_05/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12521_2022_06_21_01_49_13/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12519_2022_06_20_19_52_42/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12522_2022_06_21_03_38_01/,/media/dataRep1/RawSource/2022detectdata/锡林浩特注浆复测/400M/12520_2022_06_20_21_39_36/"

    # strPrjDir = "/media/dataRep2/processed/100989/"
    # strInPath = "/media/dataRep1/RawSource/2022detectdata/LX机场检测/12591_2022_07_18_22_06_35/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12581_2022_07_17_00_28_51/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12586_2022_07_17_20_19_23/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12578_2022_07_16_22_28_58/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12583_2022_07_17_01_12_51/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12584_2022_07_17_01_32_09/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12579_2022_07_16_23_06_34/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12585_2022_07_17_01_51_29/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12587_2022_07_17_22_18_19/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12577_2022_07_16_21_44_38/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12588_2022_07_18_02_25_19/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12580_2022_07_16_23_44_00/,/media/dataRep1/RawSource/2022detectdata/LX机场检测/12582_2022_07_17_00_54_37/,"

    strPrjDir = "/media/dataRep2/processed/100614/"
    prjID = 100614
    strInPath = '/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11213_2021_12_01_00_01_17/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11214_2021_12_01_00_23_57/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11212_2021_11_30_23_41_30/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11215_2021_12_01_02_38_55/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11209_2021_11_30_22_21_58/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11216_2021_12_01_03_28_50/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11208_2021_11_30_21_39_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11210_2021_11_30_22_44_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11211_2021_11_30_23_22_25/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11205_2021_11_29_02_02_30/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11207_2021_11_29_05_03_16/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11206_2021_11_29_03_11_18/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11201_2021_11_28_19_35_18/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11203_2021_11_29_00_18_41/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11199_2021_11_27_23_50_19/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11198_2021_11_27_21_52_04/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11200_2021_11_28_03_12_47/,/media/dataRep1/RawSource/2021detectdata/锡林浩特机场检测/11196_2021_11_27_03_21_20/,'

    imode = 9
    dtype = "2Dseg"  # 2Ddet 2Dseg 3D
    delete_database(prjID, imode, dtype)
    esTomysql(strPrjDir, strInPath, imode, dtype)