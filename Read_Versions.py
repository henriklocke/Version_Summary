

import pypyodbc #used to run Access queries

import arcpy
from arcpy import env #used to create mdb
import shutil #For copy file
import ctypes
import os # for file operations
import pandas as pd
import sqlite3
import re



def readQuery(SQL, fullPath):
    queryOutput = []
    if ".mdb" in fullPath:
        conn = pypyodbc.win_connect_mdb(fullPath)
    elif ".sqlite" in fullPath:
        conn = sqlite3.connect(fullPath)
##        print "connection established with ",fullPath
    else:
        MessageBox(None, "Tool ends." + fullPath + " not recognized as mdb or sqlite", 'Info', 0)
        exit()
    ###################
    cur = conn.cursor()
##    print SQL
    cur.execute(SQL)
    while True:
        row = cur.fetchone()
        if not row:
            break
        queryOutput.append(row)
    cur.close()
    conn.commit()
    conn.close()
    return queryOutput
##    columns = [column[0].lower() for column in cur.description]
##    return [columns,queryOutput]

working_folder = os.getcwd()

##backup_folder = r'J:\SEWER_AREA_MODELS\VSA\01_MASTER_MODEL\MODEL\Backups'
##prefix = 'VSA_BASE'

##backup_folder = r'J:\SEWER_AREA_MODELS\FSA\01_MASTER_MODEL\MODEL\BACKUP'
##prefix = 'FSA_Base'

##backup_folder = r'J:\SEWER_AREA_MODELS\NSSA\01_MASTER_MODEL\MODEL\BACKUP'
##prefix = 'NSSA_Base_Backup'

backup_folder = r'J:\SEWER_AREA_MODELS\LISA\01_MASTER_MODEL\MODEL\BACKUP'
prefix = 'LISA_Base_Backup'


##def main(working_folder,mu_path,use_accumulation):
if 1 == 1: #For debugging in idle/pyscripter uncomment this line and comment the above.

    master_list = []
    for f in os.listdir(backup_folder):

        if (f[-4:].lower()=='.mdb' or f[-7:].lower()=='.sqlite') and f[:len(prefix)].lower() == prefix.lower():
            extension = os.path.splitext(f)[1][1:].lower()
            print 'Process file ' + f
            mu_path = backup_folder + '\\' + f
            backup_no = re.findall(r'\d+', f)
            backup_no = int(backup_no[len(backup_no)-1])

            #Count nodes
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Node"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Node WHERE Active = 1"
            nodes = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Network','Number of Nodes','Number',nodes])

            #Count sealed nodes
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Node WHERE CoverTypeNo = 2"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Node WHERE CoverTypeNo = 2 AND Active = 1"
            nodes = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Network','Number of Sealed Nodes','Number',nodes])

            #Pipe length
            if extension == 'mdb':
                sql = "SELECT SUM(SHAPE_Length) FROM msm_Link"
            else:
                sql = "SELECT SUM(Length) FROM msm_Link WHERE Active = 1"
            pipe_length = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Network','Shape Length of Pipes','m',pipe_length])

            #Count pumps
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Pump"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Pump WHERE Active = 1"
            pumps = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Structures','Number of Pumps','Number',pumps])

            #Count weirs
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Weir"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Weir WHERE Active = 1"
            weirs = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Structures','Number of Weirs','Number',weirs])

            #Count orifices
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Orifice"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Orifice WHERE Active = 1"
            orifices = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Structures','Number of Orifices','Number',orifices])

            #Count valves
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM msm_Valve"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Valve WHERE Active = 1"
            valves = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Structures','Number of Valves','Number',valves])

            #Count people
            if extension == 'mdb':
                sql = "SELECT SUM(Population) FROM ms_LaLoadAlloc"
            else:
                sql = "SELECT SUM(Population) FROM msm_Loadpoint WHERE Active = 1"
            try:
                population = readQuery(sql,mu_path)[0][0]
            except:
                population = 0
            master_list.append([backup_no,'Loadpoints','Population','Number',population])

            #Sum water load
            if extension == 'mdb':
                sql = "SELECT SUM(WaterLoad)/86.4 FROM ms_LaLoadAlloc"
            else:
                sql = "SELECT SUM(Loadflow)*1000 FROM msm_Loadpoint WHERE Active = 1"
            adwf = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Loadpoints','ADWF','L/s',adwf])

            #Count catchments
            if extension == 'mdb':
                sql = "SELECT COUNT(MUID) FROM ms_Catchment"
            else:
                sql = "SELECT COUNT(MUID) FROM msm_Catchment WHERE Active = 1"
            catchments = readQuery(sql,mu_path)[0][0]
            master_list.append([backup_no,'Catchments','Number of Catchments','Number',catchments])

            if prefix[:3].lower() == 'vsa':

                #Count catchments
                if extension == 'mdb':
                    sql = "SELECT COUNT(MUID) FROM ms_Catchment WHERE NetTypeNo = 1"
                else:
                    sql = "SELECT COUNT(MUID) FROM msm_Catchment WHERE NetTypeNo = 1 AND Active = 1"
                catchments = readQuery(sql,mu_path)[0][0]
                master_list.append([backup_no,'Catchments','Number of Sanitary Catchments','Number',catchments])

                #Count catchments
                if extension == 'mdb':
                    sql = "SELECT COUNT(MUID) FROM ms_Catchment WHERE NetTypeNo = 3"
                else:
                    sql = "SELECT COUNT(MUID) FROM msm_Catchment WHERE NetTypeNo = 3 AND Active = 1"
                catchments = readQuery(sql,mu_path)[0][0]
                master_list.append([backup_no,'Catchments','Number of Combined Catchments','Number',catchments])

                #Count catchments
                if extension == 'mdb':
                    sql = "SELECT COUNT(MUID) FROM ms_Catchment WHERE NetTypeNo = 2"
                else:
                    sql = "SELECT COUNT(MUID) FROM msm_Catchment WHERE NetTypeNo = 2 AND Active = 1"
                catchments = readQuery(sql,mu_path)[0][0]
                master_list.append([backup_no,'Catchments','Number of Storm Catchments','Number',catchments])


df = pd.DataFrame(master_list,columns=['Backup_Number','Tab','Description','Unit','Value'])
df.to_csv(working_folder + '\\Backup_List.csv',index=False)

##if __name__ == "__main__":
##    print sys.argv[3]
##    main(sys.argv[1],sys.argv[2],sys.argv[3])


