# -*- coding: utf-8 -*-
# https://decentlab.squarespace.com/products/laser-distance-level-sensor-for-lorawan
from base64 import binascii
from json.encoder import py_encode_basestring
from os import path
import base64
import struct
import json
import mysql.connector as sql
import math       
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pandas as pd
import time
import numpy as np
import logging
import math

PROTOCOL_VERSION      = 2
LARGO_TRAMA_MILESIGHT = 40
LARGO_TRAMA_UC11      = 6
LARGO_TRAMA_UC300     = 7
LARGO_TRAMA_EM310     = 14
LARGO_TRAMA_SAPFLOW   = 16
APP_TIMEZONE          = -4 # (CR:20240408) Modificacion TimeZone.
logging.basicConfig(format='%(asctime)s %(message)s')

SENSORS = [
    {'length': 11,
     'values': [{'name': 'Distance: average',
                 'convert': lambda x: x[0],
                 'unit': 'mm'},
                {'name': 'Distance: minimum',
                 'convert': lambda x: x[1],
                 'unit': 'mm'},
                {'name': 'Distance: maximum',
                 'convert': lambda x: x[2],
                 'unit': 'mm'},
                {'name': 'Distance: median',
                 'convert': lambda x: x[3],
                 'unit': 'mm'},
                {'name': 'Distance: 10th percentile',
                 'convert': lambda x: x[4],
                 'unit': 'mm'},
                {'name': 'Distance: 25th percentile',
                 'convert': lambda x: x[5],
                 'unit': 'mm'},
                {'name': 'Distance: 75th percentile',
                 'convert': lambda x: x[6],
                 'unit': 'mm'},
                {'name': 'Distance: 90th percentile',
                 'convert': lambda x: x[7],
                 'unit': 'mm'},
                {'name': 'Distance: most frequent value',
                 'convert': lambda x: x[8],
                 'unit': 'mm'},
                {'name': 'Number of samples',
                 'convert': lambda x: x[9]},
                {'name': 'Total acquisition time',
                 'convert': lambda x: x[10] / 1.024,
                 'unit': 'ms'}]},
    {'length': 1,
     'values': [{'name': 'Battery voltage',
                 'convert': lambda x: x[0] / 1000,
                 'unit': 'V'}]}
]


#payload_file = r'./src/login_payload.json'
#api_paths    = r'./src/appConf.json'
#toke_file    = r'./src/token.tkn'
#file_cred    = r'./src/credentials.json'

file_cred    = r'/home/ubuntu/app/config/credentials.json'
payload_file = r'/home/ubuntu/app/config/login_payload.json'
api_paths    = r'/home/ubuntu/app/config/appConf.json'
toke_file    = r'/home/ubuntu/app/config/token.tkn'


def get_appConf():
    with open(api_paths) as file:
        paths=json.load(file)
    return(paths)


def dbConnect( sql, appCfg ):
    db = sql.connect(
        host     = appCfg['database']['host'],
        user     = appCfg['database']['user'],
        passwd   = appCfg['database']['passwd'],
        database = appCfg['database']['database']
    )
    return(db)

def convrtToEpoch( date_time ):
    date_time_str = str(date_time)
    pattern   = '%Y-%m-%d %H:%M:%S'
    n_epoch   = int(time.mktime(time.strptime(date_time_str, pattern)))
    return(n_epoch)

def DatosDL2Proccess( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('70B3D57BA00016F1','70B3D57BA00048DE','70B3D57BA00048DF','70B3D57BA00048E0','70B3D57BA00048E1','70B3D57BA00016F1','70B3D57BA00046AD','70B3D57BA00046AE','70B3D57BA00046AF') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def getDataResBuzones (db):
    #Tomo los que estan en estado = 2 porque son las medidas correctamente cerradas por el Robot, luego se deberá setear el estado = 3 para no volver a ser llamado. Este estado indica que la medida/dato esta lista para que sea procesada por la tbl_resclientes
    sql = "SELECT * FROM proyectae.pye_powerbi_res_buzones where reg_state = 0 "
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return(r)

def DatosMS2Proccess( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124128B022802','24E124128B111422') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def DatosUC112Proccess( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124122A462697','24E1612293939883') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

#(CR:20220909)Incluir UC300 al flujo
def DatosUC300Proccess( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124445C196334','24E124445C196197','24E124445C196324','24E124445C196280',  '24E124445C196445','24E124445C196354','24E124445C196432','24E124445C196319','24E124445C196372') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

#(CR:20220909)Incluir UC300 al flujo
def DatosUC300Proccess_420mA( db ):
    sql = """
    SELECT * FROM proyectae.mirror_wmc_dataup 
    where endDeviceDevEui in 
    ('24E124445C196438','24E124445D182948','24E124445D183018','24E124445D183268','24E124445D183557') 
    and reg_state = 0
    """
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

#(CR:20230110)Incluir EM310 al flujo
def DatosEM310Proccess( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124713B449667','24E124713B449121') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)


#(CR:20230402)Incluir AGRO al flujo
def DatosAgroProccess_AD_Node( db ):
    #(CR:20230724)Cambio de Nombre a la función para ser compatible con el diseño enviado por ivan en correo del 27/06/2023
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('98208E0000003401') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

#(CR:20230724)Cambio de Nombre a la función para ser compatible con el diseño enviado por ivan en correo del 27/06/2023
def DatosAgroProccess_Mrf_Node( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('98208E0000032A33') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def DatosAgroProccess_Sdi_Node( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('98208E0000032743') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

## (CR:20240923) Modificacion para incluir sensores de flujo de savia en DOLE.
def DatosAgroProccess_SFM1X( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('8C1F6460E8000079','8C1F6460E80000F5','8C1F6460E80000F6','8C1F6460E80000F7','8C1F6460E80000F8') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
def DatosAgroProccess_EM500_SMTC( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124126C062937') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def DatosAgroProccess_EM500_UDL( db ):
    sql = "SELECT * FROM proyectae.mirror_wmc_dataup where endDeviceDevEui in ('24E124126B221661') and reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)
# _______________________________________________________________________________

def decodeUC112InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_UC11:
                #print(reg[12])
                data_uc11 = decode_payload_uc11( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_uc11) > 0:
                    insert_DataProUC11(reg, data_uc11, db )
    return

def decodeUC1152_InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_UC11:
                #print(reg[12])
                data_uc11 = decode_payload_uc11( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_uc11) > 0:
                    insert_DataProUC1152(reg, data_uc11, db )
    return

def decodeUC300Pbi_InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_UC11:
                #(CR:20240305) Modificaciones para Lambda
                if reg[8] == 85 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                    #print(reg[12])
                    data_uc11 = decode_payload_uc300( reg[12] ) #paso el payload tal como viene en la DB , como string
                    if len(data_uc11) > 0:
                        insert_DataProUC1152(reg, data_uc11, db )
    return

def decodeUC3004_20mA_InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_UC11:
                #(CR:20240305) Modificaciones para Lambda
                if reg[8] == 85 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                    #print(reg[12])
                    data_uc300 = decode_payload_uc300( reg[12] ) #paso el payload tal como viene en la DB , como string
                    if len(data_uc300) > 0:
                        insert_DataUC300_4_20mA(reg, data_uc300, db )
    return

def decodeUC300InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_UC300:
                #(CR:20240305) Modificaciones para Lambda
                if reg[8] == 85 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                    #print(reg[12])
                    data_uc300 = decode_payload_uc300( reg[12] ) #paso el payload tal como viene en la DB , como string
                    if len(data_uc300) > 0:
                        insert_DataProUC11(reg, data_uc300, db )
    return


#(CR:20230110)Incluir EM310 al flujo <<<<
def decodeEM310InsertDb(datosEM, db):
    if len(datosEM) > 0:
        for reg in datosEM:
            if len(reg[12]) > LARGO_TRAMA_EM310 :
                #print(reg[12])
                data_em310 = decode_payload_em310udl( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_em310) > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataProEM310(reg, data_em310, db )
    return


#(CR:20230402)Incluir AGRO al flujo 
def decodeAgroAdNodeInsertDb(datosAgro, db):
    #(CR:20230724) Modificacion para incluir el resto de sensores
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 1 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                #print(reg[8])
                data_agro = decode_payload_sxa1mb13( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_agro) > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroAdNode(reg, data_agro, db )
    return

#(CR:20230724) Modificacion para incluir el resto de sensores
def decodeAgroMrfNodeInsertDb(datosAgro, db):    
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 1 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                #print(reg[8])
                data_agro = decode_payload_mnla4n101( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_agro) > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroMrfNode(reg, data_agro, db )
    return

def decodeAgroSdiNodeInsertDb(datosAgro, db):    
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 1 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                #print(reg[8])
                data_agro = decode_payload_snla2n204( reg[12] ) #paso el payload tal como viene en la DB , como string
                if len(data_agro) > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroSdiNode(reg, data_agro, db )
    return

# (CR:20240923) Modificacion para incluir sensor de flujo de savia.
def decodeAgroSFM1XInsertDb(datosAgro, db):    
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 1 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                if len(reg[12]) > LARGO_TRAMA_SAPFLOW:
                    data_agro = decode_payload_sxa1mb13_sfm1x( reg[12] ) #Esto es para procesar los datos de la versión antigua de Sapflow
                else:
                    data_agro = decode_payload_sxa1mb13_sfm1x_fwR1_0_16( reg[12], fPort=1) #Esto es para procesar los datos de la versión nueva de Sapflow

                if data_agro['header'] > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroSfm1x(reg, data_agro, db )
    return

#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
def decodeAgroEM500smtcInsertDb(datosAgro, db):    
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 85 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                # print(reg[8])
                data_agro = decode_payload_EM500_SMTC( reg[12] ) #paso el payload tal como viene en la DB , como string
                if data_agro['channel_id'] > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroEm500(reg, data_agro, db ,'SMTC')
    return

def decodeAgroEM500udlInsertDb(datosAgro, db):    
    if len(datosAgro) > 0:
        for reg in datosAgro:
            if reg[8] == 85 : # Hago la consulta por el puerto, para que procese sólo la info que trae data valida
                # print(reg[8])
                data_agro = decode_payload_EM500_UDL( reg[12] ) #paso el payload tal como viene en la DB , como string
                if data_agro['channel_id'] > 0: # Esto significa que tengo datos válidos que procesar!!!
                    insert_DataAgroEm500(reg, data_agro, db , 'UDL')
    return
#________________________________________________________________________________
#(CR:20230724) Modificacion para incluir el resto de sensores
def insert_DataAgroMrfNode( reg, data_agro, db ):
        
    sql ="INSERT INTO proyectae.pye_proc_agro_mrfnode (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`datain_timestamp`,`reg_state`,`endDeviceDevEui`,`recvTime`,`packet-type`,`payload-version`,`charging-state`,`fault`,`header`,`uptime`,`battery-voltage`,`solar-voltage`,`frequency`,`voltage-adc-1`,`voltage-adc-2`,`voltage-adc-3`,`voltage-adc-4`,`digital-count-4`,`digital-count-3`,`digital-count-2`,`digital-count-1`,`precipitation`,`command`,`air-temperature`,`relative-humidity`, `vapour-pressure-deficit`,`barometric-pressure`,`saturated-vapour-pressure`,`vapour-pressure`,`dew-point-temperature`,`water-vapour-density`,`solar-radiation`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    #print(f"data_agro:{data_agro}")
    nombre_dispositivo=''
    if reg[6] in ['8C1F6460E8000079','8C1F6460E80000F5', '8C1F6460E80000F6', '8C1F6460E80000F7', '8C1F6460E80000F8']:
        nombre_dispositivo = "MRF"

    ts        = reg[17] / 1000.0
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    

    val = (
            reg[0],
            0,
            nombre_dispositivo,
            reg[1],
            0, 
            reg[6],
            timestamp,
            data_agro['packet-type'],
            data_agro['payload-version'],
            data_agro['charging-state'],
            data_agro['fault'], 
            data_agro['header'],
            data_agro['uptime'],
            data_agro['battery-voltage'],
            data_agro['solar-voltage'],  
            data_agro['frequency'], 
            data_agro['voltage-adc-1'], 
            data_agro['voltage-adc-2'], 
            data_agro['voltage-adc-3'], 
            data_agro['voltage-adc-4'], 
            data_agro['digital-count-4'], 
            data_agro['digital-count-3'], 
            data_agro['digital-count-2'], 
            data_agro['digital-count-1'], 
            data_agro['precipitation'], 
            data_agro['command'], 
            data_agro['air-temperature'], 
            data_agro['relative-humidity'], 
            data_agro['vapour-pressure-deficit'], 
            data_agro['barometric-pressure'], 
            data_agro['saturated-vapour-pressure'], 
            data_agro['vapour-pressure'], 
            data_agro['dew-point-temperature'],
            data_agro['water-vapour-density'], 
            data_agro['solar-radiation'], 
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return

def insert_DataAgroSdiNode( reg, data_agro, db ):
        
    sql ="INSERT INTO proyectae.pye_proc_agro_sdinode (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`datain_timestamp`,`reg_state`,`endDeviceDevEui`,`recvTime`,`packet-type`,`uptime`,`battery-voltage`,`solar-voltage`,`charging-state`,`fault`,`gnss`,`latitude`,`longitude`,`command`,`soil-moisture-1`,`soil-moisture-2`,`soil-moisture-3`,`soil-moisture-4`,`soil-moisture-5`,`soil-moisture-6`,`soil-moisture-7`,`soil-moisture-8`,`soil-temperature-1`,`soil-temperature-2`,`soil-temperature-3`,`soil-temperature-4`,`soil-temperature-5`,`soil-temperature-6`,`soil-temperature-7`,`soil-temperature-8`,`circumference-change`,`sensor-temperature`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    #print(f"data_agro:{data_agro}")
    nombre_dispositivo=''
    if reg[6] == "98208E0000032743":
        nombre_dispositivo= "DSI-12"

    ts        = reg[17] / 1000.0
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    

    val = (
            reg[0],
            0,
            nombre_dispositivo,
            reg[1],
            0, 
            reg[6],
            timestamp,
            data_agro['packet-type'],
            data_agro['uptime'],
            data_agro['battery-voltage'],
            data_agro['solar-voltage'],
            data_agro['charging-state'],
            data_agro['fault'],
            data_agro['gnss'],
            data_agro['latitude'],
            data_agro['longitude'],
            data_agro['command'],
            data_agro['soil-moisture-1'],
            data_agro['soil-moisture-2'],
            data_agro['soil-moisture-3'],
            data_agro['soil-moisture-4'],
            data_agro['soil-moisture-5'],
            data_agro['soil-moisture-6'],
            data_agro['soil-moisture-7'],
            data_agro['soil-moisture-8'],
            data_agro['soil-temperature-1'],
            data_agro['soil-temperature-2'],
            data_agro['soil-temperature-3'],
            data_agro['soil-temperature-4'],
            data_agro['soil-temperature-5'],
            data_agro['soil-temperature-6'],
            data_agro['soil-temperature-7'],
            data_agro['soil-temperature-8'],
            data_agro['circumference-change'],
            data_agro['sensor-temperature'],
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return

#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
def insert_DataAgroEm500( reg, data, db , sensor_type):

    sql ="INSERT INTO proyectae.pye_proc_agro_em500 (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`datain_timestamp`,`reg_state`,`endDeviceDevEui`,`recvTime`,`sensor_type`,`channel_id`,`channel_type`,`battery`,`moisture`,`ec`,`temperature`,`temperature_change`,`temperature_alarm`,`timestamp`,`distance`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    nombre_dispositivo=''
    data_em500 = struct_data_em500.copy()

    if sensor_type=='SMTC':
        if reg[6] == "24E124126C062937":
            nombre_dispositivo= "EM500-SMTC"

        data_em500['sensor_type'] = 'SMTC'
        data_em500['channel_id'] = data['channel_id'] 
        data_em500['channel_type'] = data['channel_type']
        data_em500['battery'] = data['battery']
        data_em500['moisture'] = data['moisture']
        data_em500['ec'] = data['ec']
        data_em500['temperature'] = data['temperature']
        data_em500['temperature_change'] = data['temperature_change']
        data_em500['temperature_alarm'] = data['temperature_alarm']
        data_em500['timestamp'] = data['timestamp'] 

    elif sensor_type=='UDL':
        if reg[6] == "24E124126B221661":
            nombre_dispositivo= "EM500-Ultrasónico"

        data_em500['sensor_type'] = 'UDL'
        data_em500['channel_id'] = data['channel_id'] 
        data_em500['channel_type'] = data['channel_type']
        data_em500['battery'] = data['battery']
        data_em500['timestamp'] = data['timestamp'] 
        data_em500['distance'] = data['distance'] 

    val = (
            reg[0],
            0,
            nombre_dispositivo,
            reg[1],
            0,
            reg[6],
            timestamp,
            data_em500['sensor_type'],
            data_em500['channel_id'],
            data_em500['channel_type'],
            data_em500['battery'],
            data_em500['moisture'],
            data_em500['ec'],
            data_em500['temperature'],
            data_em500['temperature_change'],
            data_em500['temperature_alarm'],
            data_em500['timestamp'],
            data_em500['distance'],
            )

    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return

# (CR:20231206) Modificacion para incluir sensor de flujo de savia.
def insert_DataAgroSfm1x( reg, data_agro, db ):

    sql ="INSERT INTO proyectae.pye_proc_agro_sfm1x (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`site`,`datain_timestamp`,`reg_state`,`endDeviceDevEui`,`recvTime`,`packet-type`,`uncorrected-outer`,`uncorrected-inner`,`corrected-outer`,`corrected-inner`,`sap-flow-outer`,`sap-flow-inner`,`battery-voltage`,`battery-charge-current`,`internal-battery-temp`,`external-power-supply-present`,`external-power-supply-voltage`,`external-power-supply-current`,`fault`, `header`,`max-temp-downstream-outer`,`max-temp-upstream-outer`,`rise-temp-downstream-outer`,`rise-temp-upstream-outer`,`ratio-outer`,`max-temp-downstream-inner`,`max-temp-upstream-inner`,`rise-temp-downstream-inner`,`rise-temp-upstream-inner`,`ratio-inner`,`pulse-duration`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    #print(f"data_agro:{data_agro}")
    nombre_dispositivo=''
    if reg[6] == "98208E0000032743":
        nombre_dispositivo= "Flujo Savia"

    site='Campo'
    
    # ts        = reg[17] / 1000.0
    # timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    

    val = (
            reg[0],
            0,
            nombre_dispositivo,
            site,
            reg[1],
            0,
            reg[6],
            timestamp,
            data_agro['packet-type'],
            data_agro['uncorrected-outer'],
            data_agro['uncorrected-inner'],
            data_agro['corrected-outer'],
            data_agro['corrected-inner'],
            data_agro['sap-flow-outer'],
            data_agro['sap-flow-inner'],
            data_agro['battery-voltage'],
            data_agro['battery-charge-current'],
            data_agro['internal-battery-temp'],
            data_agro['external-power-supply-present'],
            data_agro['external-power-supply-voltage'],
            data_agro['external-power-supply-current'],
            data_agro['fault'],
            data_agro['header'],
            data_agro['max-temp-downstream-outer'],
            data_agro['max-temp-upstream-outer'],
            data_agro['rise-temp-downstream-outer'],
            data_agro['rise-temp-upstream-outer'],
            data_agro['ratio-outer'],
            data_agro['max-temp-downstream-inner'],
            data_agro['max-temp-upstream-inner'],
            data_agro['rise-temp-downstream-inner'],
            data_agro['rise-temp-upstream-inner'],
            data_agro['ratio-inner'],
            data_agro['pulse-duration'],
            )

    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return


def insert_DataAgroAdNode( reg, data_agro, db ):
        
    sql =" INSERT INTO proyectae.pye_proc_agro_adnode (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`datain_timestamp`,`reg_state`,`endDeviceDevEui`,`recvTime`,`uptime`,`battery-voltage`,`current-adc`,`voltage-adc`,`temperature_1`,`temperature_2`,`digital-count_4`,`digital-count_3`,`digital-count_2`,`digital-count_1`,`accelerometer-x`,`accelerometer-y`,`accelerometer-z`,`x_offset`,`y_offset`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    print("reg[6]:",reg[6])
    nombre_dispositivo=''
    if reg[6] == "98208E0000003401":
        nombre_dispositivo= "ADLA1L301"        

    # ts        = reg[17] / 1000.0
    # timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    

    val = (
            reg[0],
            0,
            nombre_dispositivo,
            reg[1],
            0, 
            reg[6],
            timestamp,
            data_agro['uptime'],
            data_agro['battery-voltage'],
            data_agro['current-adc'],
            data_agro['voltage-adc'],
            data_agro['temperature_1'],
            data_agro['temperature_2'],
            data_agro['digital-count_4'],
            data_agro['digital-count_3'],
            data_agro['digital-count_2'],
            data_agro['digital-count_1'],
            data_agro['accelerometer-x'],
            data_agro['accelerometer-y'],
            data_agro['accelerometer-z'],
            data_agro['x_offset'],
            data_agro['y_offset'],
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return


def insert_DataProEM310( reg, data_em310, db ):
        
    sql =" INSERT INTO proyectae.pye_powerbi_em310 (`id_dataup`,`reg_state`,`endDeviceDevEui`,`id_dispositivo`,`nombre_dispositivo`,`recvTime`,`battery`,`distance`,`position`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #(CR:20230522) Modificacion para reparar el TimeStamp de acuerdo al timezone, estaba bien antes pero por error lo cambie. Ahora vuelve al estado correcto. 
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    if reg[6] == "24E124713B449667":
        nombre_dispositivo= "EM310 - Ultrasónico 1"        

    if reg[6] == "24E124713B449121":        
        nombre_dispositivo= "EM310 - Ultrasónico 2"

    val = (
            reg[0],
            0,
            reg[6],
            reg[6],
            nombre_dispositivo,
            timestamp,
            data_em310[0][1],
            data_em310[1][1],
            data_em310[2][1],
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return





#(CR:20230110)Fin Modificacion EM310  >>>






def insert_DataProUC11( reg, data_uc11, db ):

    din = []
    for d in data_uc11:
        din.append(d[0])
        din.append(d[1])
        din.append(d[2])
    
    #print(range(6-len(data_uc11)))
    
    for k in range(6-len(data_uc11)):
        din.append(0)
        din.append(0)
        din.append(0)
    
    
    sql =" INSERT INTO proyectae.pye_powerbi_uc11 (`id_dataup`,`reg_state`,`endDeviceDevEui`,`payload`,`recvTime`,`modbus_chn_id_1`,`channel_type_1`,`data_1`,`modbus_chn_id_2`,`channel_type_2`,`data_2`,`modbus_chn_id_3`,`channel_type_3`,`data_3`,`modbus_chn_id_4`,`channel_type_4`,`data_4`,`modbus_chn_id_5`,`channel_type_5`,`data_5`,`modbus_chn_id_6`,`channel_type_6`,`data_6`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                       
    # ts        = reg[17] / 1000.0
    # timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )

    val = (
            reg[0],
            0,
            reg[6],
            reg[12],
            timestamp,
            din[0],
            din[1],
            din[2],
            din[3],
            din[4],
            din[5],
            din[6],
            din[7],
            din[8],
            din[9],
            din[10],
            din[11],
            din[12],
            din[13],
            din[14],
            din[15],
            din[16],
            din[17],
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return

def insert_DataUC300_4_20mA( reg, data, db ):
    din = []
    TIMEZONE = APP_TIMEZONE

    # Inicializar variables
    analogInCh1_4_20mA = None
    analogInCh2_4_20mA = None
    analogInCh1_0_10V = None
    analogInCh2_0_10V = None

    # Recorrer cada registro en el resultado
    for record in data:
        # print(f'record: {record} \t record[0]: {record[0]} \t record[1]: {record[1]} \t record[2]: {record[2]}')
        if record[0] == 11:
            analogInCh1_4_20mA = record[2]
        elif record[0] == 12:
            analogInCh2_4_20mA = record[2]
        elif record[0] == 13:
            analogInCh1_0_10V = record[2]
        elif record[0] == 14:
            analogInCh2_0_10V = record[2]
    
    sql =""" 
    INSERT INTO proyectae.pye_powerbi_uc300 
    (`id_dataup`,`id_dispositivo`,`nombre_dispositivo`,`payload`,
    `endDeviceDevEui`,`recvTime`,`analogInCh1_4_20mA`,`analogInCh2_4_20mA`,
    `analogInCh1_0_10V`,`analogInCh2_0_20V`) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """   
    timestamp = utc_change_zone(reg[17], TIMEZONE )

    if reg[6] == "24E124445C196438":
        nombre_dispositivo= "UC300 - UD@4 - Abbott"        

    if reg[6] == "24E124445D182948":        
        nombre_dispositivo= "UC300 - UD@5 - Abbott"

    if reg[6] == "24E124445D183018":        
        nombre_dispositivo= "UC300 - UD@2- Abbott"
    
    if reg[6] == "24E124445D183268":        
        nombre_dispositivo= "UC300 - UD@1 - Abbott"

    if reg[6] == "24E124445D183557":        
        nombre_dispositivo= "UC300 - UD@3 - Abbott"

    val = (
            reg[0],
            reg[3],
            nombre_dispositivo,
            reg[12],
            reg[6],
            timestamp,
            analogInCh1_4_20mA,
            analogInCh2_4_20mA,
            analogInCh1_0_10V,
            analogInCh2_0_10V,
        )

    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return


def insert_DataProUC1152( reg, data_uc11, db ):
    din = []
    TIMEZONE = APP_TIMEZONE
    for d in data_uc11:
        din.append(d[0])
        din.append(d[1])
        din.append(d[2])
    
    #print(range(6-len(data_uc11)))
    
    for k in range(6-len(data_uc11)):
        din.append(0)
        din.append(0)
        din.append(0)
       
    sql =" INSERT INTO proyectae.pye_powerbi_uc1152 (`id_dataup`,`reg_state`,`endDeviceDevEui`,`id_dispositivo`,`nombre_dispositivo`,`recvTime`,`modbus_chn_id_3`,`channel_type_3`,`data_3`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    #ts        = reg[17] / 1000.0
    #timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')    
    timestamp = utc_change_zone(reg[17], TIMEZONE )

    if reg[6] == "24E124122A462697":
        nombre_dispositivo= "Flujómetro Supmea"        

    if reg[6] == "24E1612293939883":        
        nombre_dispositivo= "Flujómetro Holykell"

    if reg[6] == "24E124445C196334":        
        nombre_dispositivo= "FIT-01 - Empalme A"
    
    if reg[6] == "24E124445C196197":        
        nombre_dispositivo= "FIT-02 - Empalme B"

    if reg[6] == "24E124445C196324":        
        nombre_dispositivo= "FIT-03 - Empalme C"

    if reg[6] == "24E124445C196280":        
        nombre_dispositivo= "FIT-04 - Empalme D"

    if reg[6] == "24E124445C196445":        
        nombre_dispositivo= "FIT-05 - General Entrada Fuentes Maturana"

    if reg[6] == "24E124445C196354":        
        nombre_dispositivo= "FIT-06 - E-2 SO3 Entrada a Cisterna #3"

    if reg[6] == "24E124445C196432":        
        nombre_dispositivo= "FIT-07 - E-6 Entrada a Cisternas #1 y #2"

    if reg[6] == "24E124445C196319":        
        nombre_dispositivo= "FIT-08 - E-1 Entrada a Casino"

    if reg[6] == "24E124445C196372":        
        nombre_dispositivo= "FIT-09 - E-6 Entrada a UMAS"

    val = (
            reg[0],
            0,
            reg[6],
            reg[6],
            nombre_dispositivo,
            timestamp,
            din[6],
            din[7],
            din[8],
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return


def getDataProccessDL( db , endDevice ):
    cursor =  db.cursor()

    if len(endDevice)>0:
        sql = "SELECT * FROM proyectae.pye_proc_decentlab where reg_state = 0 and endDeviceDevEui = %s"
        #sql = "SELECT * FROM proyectae.pye_proc_decentlab where endDeviceDevEui = %s"
        val = (endDevice,)
        cursor.execute(sql, val)
    else:
        sql = "SELECT * FROM proyectae.pye_proc_decentlab where reg_state = 0"
        cursor.execute(sql)    

    r = cursor.fetchall()
    return (r)

def getDataProccessMS( db ):
    sql = "SELECT * FROM proyectae.pye_proc_milesight where reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def getCantidadBuzones( db ):
    sql = "SELECT id_buzon as buzones, count(*) FROM proyectae.pye_powerbi_datos_nodos group by buzones"
    cursor =  db.cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    return (r)

def getAllDataBuzon( db , id_buzon ):
    #sql = "SELECT * FROM proyectae.pye_powerbi_datos_nodos where id_buzon = 'QUI-02' and fecha_hora_lectura_UTC > '2021.11.05 00:00:00' order by fecha_hora_lectura_UTC asc"
    sql = "SELECT * FROM proyectae.pye_powerbi_datos_nodos where id_buzon = %s and reg_state = 0 order by fecha_hora_lectura_UTC asc;"
    val = (id_buzon,)
    cursor =  db.cursor()
    cursor.execute( sql , val )
    r = cursor.fetchall()
    return (r)

def decodeDecentlab2InsertDb(datosDL, db):
    if len(datosDL) > 0:
        for reg in datosDL:
            #print(reg[12])
            decoPay = decoPayloadDecentlab( base64.b64decode(reg[12]).hex() , hex=True)
            #pprint.pprint(decoPay)
            insert_DataPreProDL( reg , decoPay , db )
    #else:
    #    print (datosDL)
    return

def decodeMilesight2InsertDb(datosMS, db):
    if len(datosMS) > 0:
        for reg in datosMS:
            if len(reg[12]) > LARGO_TRAMA_MILESIGHT:
                #print(reg[12])
                battery, temp,hum,co2, activity,lum, vis,infra,tvoc, barometric = data_payload( base64.b64decode(reg[12]).hex() )
                #pprint.pprint(decoPay)
                insert_DataPreProMS( reg , battery, temp,hum,co2, activity,lum, vis,infra,tvoc, barometric, db )
    #else:
    #    print (datosMS)
    return


def insertDataMS2tblPowerBI (datosMS, db):

    if len(datosMS) > 0:
        cursor =  db.cursor()
        for reg in datosMS:
            sql =" INSERT INTO proyectae.pye_powerbi_milesight (`id_dataup`,`reg_state`,`endDeviceDevEui`,`recvTime`,`battery`,`temp`,`hum`,`co2`,`activity`,`lum`,`vis`,`infra`,`tvoc`,`barometric`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            #(CR:20230522) Aqui debo modificar el timezone para que pueda ser visto por el resto de las funciones.
            #ts        = reg[10] / 1000.0
            #timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            timestamp = utc_change_zone(reg[10], APP_TIMEZONE)
            val = (
                    reg[1],
                    0,
                    reg[7],
                    timestamp,
                    reg[12],
                    reg[13],
                    reg[14],
                    reg[15],
                    reg[16],
                    reg[17],
                    reg[18],
                    reg[19],
                    reg[20],
                    reg[21]
            )
            cursor.execute(sql, val)
            db.commit()
    return



def insertDataDL2tblPowerBI (datosDL, db):
    #print(datosDL)
    if len(datosDL) > 0:
        cursor =  db.cursor()
        for reg in datosDL:
            sql =" INSERT INTO proyectae.pye_powerbi_decentlab (`id_dataup`,`reg_state`,`endDeviceDevEui`,`recvTime`,`batteryVoltageUnit`,`batteryVoltageValue`,`dlDeviceID`,`distance10p`,`distance25p`,`distance75p`,`distance90p`,`distanceAverage`,`distanceMaximum`,`distanceMedian`,`distanceMinimum`,`distanceMostFrequentValue`,`numberOfSamples`,`protocolVersion`,`totalAcquisitionTime`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            ts        = reg[10] / 1000.0
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            val = (
                    reg[1],
                    0,
                    reg[7],
                    timestamp,
                    reg[12],
                    reg[13],
                    reg[14],
                    reg[15],
                    reg[16],
                    reg[17],
                    reg[18],
                    reg[19],
                    reg[20],
                    reg[21],
                    reg[22],
                    reg[23],
                    reg[24],
                    reg[25],
                    reg[26]                
            )
            cursor.execute(sql, val)
            db.commit()
    return


def insert_DataPreProMS( reg , battery, temp,hum,co2, activity,lum, vis,infra,tvoc, barometric, db ):
    #(CR:20230522) Modificacion para agregar TimeZone correcto al dato. Aqui se agregó una nueva columna extra a la DB para incluir el dato en formato timestamp
    sql =" INSERT INTO proyectae.pye_proc_milesight (`id_dataup`,`reg_state`,`id`,`endDeviceDevAddr`,`endDeviceClusterId`,`endDeviceDevEui`,`fPort`,`payload`,`recvTime`,`gwRecvTime`,`battery`,`temp`,`hum`,`co2`,`activity`,`lum`,`vis`,`infra`,`tvoc`,`barometric`, `recvTime_tz`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    timestamp = utc_change_zone(reg[17], APP_TIMEZONE )
    val = (
            reg[0],
            0,
            reg[3],
            reg[4],
            reg[5],
            reg[6],
            reg[8],
            reg[12],
            reg[17],
            reg[18],
            battery,
            temp,
            hum,
            co2,
            activity,
            lum,
            vis,
            infra,
            tvoc,
            barometric,
            timestamp,
        )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    return

def insert_DataPreProDL( reg, decPay, db):
    try:
        sql =" INSERT INTO proyectae.pye_proc_decentlab (`id_dataup`,`reg_state`,`id`,`endDeviceDevAddr`,`endDeviceClusterId`,`endDeviceDevEui`,`fPort`,`payload`,`recvTime`,`gwRecvTime`,`batteryVoltageUnit`,`batteryVoltageValue`,`dlDeviceID`,`distance10p`,`distance25p`,`distance75p`,`distance90p`,`distanceAverage`,`distanceMaximum`,`distanceMedian`,`distanceMinimum`,`distanceMostFrequentValue`,`numberOfSamples`,`protocolVersion`,`totalAcquisitionTime`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # (CR:20230522) Elimina Log
        # print("decPay:",decPay) 
        # print("endDeviceDevEui",reg[6])
        # print("\n\r")
        val = (
                reg[0],
                0,
                reg[3],
                reg[4],
                reg[5],
                reg[6],
                reg[8],
                reg[12],
                reg[17],
                reg[18],
                decPay['Battery voltage']['unit'],
                decPay['Battery voltage']['value'],
                decPay['Device ID'],
                decPay['Distance: 10th percentile']['value'],
                decPay['Distance: 25th percentile']['value'],
                decPay['Distance: 75th percentile']['value'],
                decPay['Distance: 90th percentile']['value'],
                decPay['Distance: average']['value'],
                decPay['Distance: maximum']['value'],
                decPay['Distance: median']['value'],
                decPay['Distance: minimum']['value'],
                decPay['Distance: most frequent value']['value'],
                decPay['Number of samples']['value'],
                decPay['Protocol version'],
                decPay['Total acquisition time']['value']
            )
        cursor =  db.cursor()
        cursor.execute(sql, val)
        db.commit()
    except:
        # (CR:20230522) Elimina Log
        print("Error en Procesamiento de Trama DL")
        print("decPay:",decPay) 
        print("endDeviceDevEui",reg[6])
        print("\n\r")
        return       

    return

def updateRegstatetblDatosNodos ( db , id_reg , reg_est):
    #print("id_reg:",id_reg," estado:",reg_est)
    sql = "UPDATE proyectae.pye_powerbi_datos_nodos SET reg_state = %s where reg_state = 0  and id_pye_powerbi_datos_nodos = %s"
    val = ( reg_est, int(id_reg))
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()
    #print(cursor.statement) #Sirve para validar la ultima consulta que se ejecuto
    return

def updateRegstatetblProccess ( db , tipo):
    if tipo =='Decentlab':
        sql = "UPDATE proyectae.pye_proc_decentlab SET reg_state = 1 where reg_state = 0"
    else:
        sql = "UPDATE proyectae.pye_proc_milesight SET reg_state = 1 where reg_state = 0"
    cursor =  db.cursor()
    cursor.execute(sql)
    db.commit()
    return

def updateRegstateResBuzones( db, estado):

    sql = "UPDATE proyectae.pye_powerbi_res_buzones SET reg_state = %s where reg_state = 0"

    val = (estado, )
    cursor =  db.cursor()
    cursor.execute(sql, val)
    db.commit()

    return

def updateRegstateDataup(db, tipo):

    if tipo =='Decentlab':
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('70B3D57BA00016F1','70B3D57BA00048DE','70B3D57BA00048DF','70B3D57BA00048E0','70B3D57BA00048E1','70B3D57BA00016F1','70B3D57BA00046AD','70B3D57BA00046AE','70B3D57BA00046AF') and reg_state = 0"
    elif tipo =='Milesight':
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('24E124128B022802','24E124128B111422') and reg_state = 0"
    elif tipo =='UC11' :
        #(CR:20220208) Aqui va la query para actualizar el estado del registro
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('24E124122A462697','24E1612293939883') and reg_state = 0"
    elif tipo =='EM310':
        #(CR:20230110) Aqui va la query para actualizar el estado del registro
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('24E124713B449667','24E124713B449121') and reg_state = 0"
    elif tipo =='AGRO_AdNode':
        #(CR:20230402) Aqui va la query para actualizar el estado del registro
        #(CR:20230724) Modificacion para incluir el resto de los sensores
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('98208E0000003401') and reg_state = 0"
    elif tipo =='AGRO_MrfNode':
        #(CR:20230724) Modificacion para incluir el resto de los sensores
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('98208E0000032A33') and reg_state = 0"
    elif tipo =='AGRO_SdiNode':
        #(CR:20230724) Modificacion para incluir el resto de los sensores
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('98208E0000032743') and reg_state = 0"
    elif tipo =='AGRO_SFM1X':
        #(CR:20240923) Modificacion para incluir los sensores de flujo de savia instalados en DOLE.
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('8C1F6460E8000079','8C1F6460E80000F5','8C1F6460E80000F6','8C1F6460E80000F7','8C1F6460E80000F8') and reg_state = 0"
    elif tipo =='AGRO_EM500_SMTC':
        #(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('24E124126C062937') and reg_state = 0"
    elif tipo =='AGRO_EM500_UDL':
        #(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
        sql = "UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 where endDeviceDevEui in ('24E124126B221661') and reg_state = 0"
    elif tipo =='UC300_4_20mA':
        #(CR:20240927) Modificaciones para incluir uc300 en 4-20mA
        sql = """ UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 
        where 
        endDeviceDevEui in 
        ('24E124445C196438','24E124445D182948','24E124445D183018','24E124445D183268','S')  
        and reg_state = 0 """
    else:
        #(CR:20220809) Aqui va la query para actualizar el estado del registro de los dispositivos UC300
        sql = """ UPDATE proyectae.mirror_wmc_dataup SET reg_state = 1 
        where 
        endDeviceDevEui in ('24E124445C196334','24E124445C196197', '24E124445C196324','24E124445C196280', 
        '24E124445C196445','24E124445C196354','24E124445C196432','24E124445C196319','24E124445C196372') and reg_state = 0 """
        
    cursor =  db.cursor()
    cursor.execute(sql)
    db.commit()
    return

def decoPayloadDecentlab(msg, hex=False):
    """msg: payload as one of hex string, list, or bytearray"""
    bytes_ = bytearray(binascii.a2b_hex(msg)
                       if hex
                       else msg)
    version = bytes_[0]
    if version != PROTOCOL_VERSION:
        raise ValueError("protocol version {} doesn't match v2".format(version))

    devid = struct.unpack('>H', bytes_[1:3])[0]
    bin_flags = bin(struct.unpack('>H', bytes_[3:5])[0])
    flags = bin_flags[2:].zfill(struct.calcsize('>H') * 8)[::-1]

    words = [struct.unpack('>H', bytes_[i:i + 2])[0]
             for i
             in range(5, len(bytes_), 2)]

    cur = 0
    result = {'Device ID': devid, 'Protocol version': version}
    for flag, sensor in zip(flags, SENSORS):
        if flag != '1':
            continue

        x = words[cur:cur + sensor["length"]]
        cur += sensor["length"]
        for value in sensor['values']:
            if 'convert' not in value:
                continue

            result[value['name']] = {'value': value['convert'](x),
                                     'unit': value.get('unit', None)}

    return result

def binario_a_ascii(binario):
    # Convertir binario a decimal
    valor = int(binario, 2)
    # Convertir el decimal a su representación ASCII
    return chr(valor)

def binario_a_texto(texto_binario):
    texto_plano = ""
    separador = "x"
    for binario in texto_binario.split(separador):
        texto_plano += binario_a_ascii(binario)
    return texto_plano

# *************************************
#  HASTA AQUI EL DECODER DE DECENTLAB
# *************************************



# *************************************
#  AQUI COMIENZA  EL  DECODER DEL UC11
# *************************************
def decoder_uc11_sensor_data(bytes):
    channel_id   = bytes[0]
    channel_type = bytes[1]

    if channel_id == 0xFF and channel_type == 0x0E:
        modbus_chn_id = bytes[2]
        package_type = bytes[3]
        data_type = package_type & 7
        date_length = package_type >> 3

        if data_type ==0:
            decoded = bytes[4]
        elif data_type ==1:
            decoded = bytes[4]
        elif data_type == 2 or data_type == 3:
            decoded = readUInt16LE (bytes[4: 6])
        elif data_type==4 or data_type==6:    
            decoded = readUInt32LE (bytes[4: 8])
        elif data_type==5 or data_type==7:    
            decoded = readFloatLE (bytes[4: 8])
    if pd.isna(decoded) or math.isinf(decoded):
        #print (decoded)
        decoded=0
    return([ modbus_chn_id, channel_type, decoded])

def readUInt16LE(bytes):
    value = (bytes[1] << 8) + bytes[0]
    return (value & 0xFFFF)

#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
def readInt16LE(bytes):
    ref = readUInt16LE(bytes)
    return ref - 0x10000 if ref > 0x7fff else ref

def readUInt32LE(bytes): 
    value = (bytes[3] << 24) + (bytes[2] << 16) + (bytes[1] << 8) + bytes[0]
    return (value & 0xFFFFFFFF)


def readInt32LE(bytes):
    ref = readUInt32LE(bytes)
    if ref > 0x7FFFFFFF:
        return(ref - 0x100000000)
    return (ref)

def readFloatLE(bytes):
    str_val=''
    for a in reversed(bytes):
        str_val = str_val+f'{a:02X}'
        #print(f'{a:02X}')
    
    #print(''.join(f'{i:02x}' for i in bytes))
    f = struct.unpack('!f', bytes.fromhex(str_val))[0]   

    # print("py:",bytes.hex())
    # print("my:",str_val)
    return f


def decode_payload_uc11(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)
    #print("Payload Raw:",payload_str)
    #print("Payload Hex:",payload_hex.hex())
    decoded_out = []
    k = 0
    i = 0
    while k < len(payload_hex):
        if payload_hex[i]==0xff and payload_hex[i+1]==0x0e:
            package_type = payload_hex[i+3]
            data_length  = package_type >> 3
            k            = i + 3 + data_length + 1
            data_sensor = payload_hex [i:k]

            #print("data_length:",data_length)
            #print(data_sensor.hex())

            decoded_out.append(decoder_uc11_sensor_data(data_sensor))

            i = k
        elif len(payload_hex)>6: #Esto implica que existen mas datos juntos en la misma trama
            i = i + 1
            k = i
        else:
            k = len(payload_hex) + 1
    return decoded_out


# *************************************
#  AQUI COMIENZA  EL  DECODER DEL UC300
# *************************************
def decoder_uc300_sensor_data(bytes):
    channel   = bytes[0]
    type      = bytes[1]
    decoded   = np.NaN  # (CR:20230522) Incluir condición inicial para que no se caiga el programa.
    #print("Data UC300:", bytes.hex())
    if channel == 0xFF and type == 0x19:
        modbus_chn_id = bytes[2]
        data_len      = bytes[3]
        data_type     = bytes[4]
        value         = bytes[5: 5 + data_len + 1]
        #print("data_len: ", data_len)
        #print("data_type: ", data_type)
        #print("value: ", value.hex())
        #print("Len Bytes:", len(value))
        
        if data_type ==0:
            decoded = bytes[5]
        elif data_type ==1:
            decoded = bytes[5]
        elif data_type == 2 or data_type == 3:
            decoded = readUInt16LE (value) #(bytes[5: 5 + data_len + 1])
        elif data_type==4 or data_type==6:    
            decoded = readUInt32LE (value) #(bytes[5: 5 + data_len + 1])
        elif data_type==5 or data_type==7:    
            decoded = readFloatLE (value) #(bytes[5: 5 + data_len + 1])
        #print ("Ch:", modbus_chn_id," value: ",decoded)
    if pd.isna(decoded) or math.isinf(decoded):        
        decoded=0
    return([ modbus_chn_id, type, decoded])

def decode_payload_uc300(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)
    # print("(CR)Payload Raw:",payload_str)
    # print("Payload Hex:",payload_hex.hex())
    decoded_out = []
    k = 0
    i = 0
    while k < len(payload_hex):
        if payload_hex[i]==0xff and payload_hex[i+1]==0x19:
            data_length  = payload_hex[i+3]
            k            = i + 3 + 1 + data_length + 1
            data_sensor = payload_hex [i:k]

            #print("data_length:",data_length)
            #print(data_sensor.hex())

            decoded_out.append(decoder_uc300_sensor_data(data_sensor))

            i = k
        elif (payload_hex[i] == 0x0b or payload_hex[i] == 0x0c) and payload_hex[i+1] == 0x02:
            value = payload_hex[i+2:i+6]
            decoded = readUInt32LE (value)
            mA = decoded * 0.01
            decoded_out.append([payload_hex[i], payload_hex[i+1], mA])
            k = i + 6 
            i = k
            # print(f'decode:{decoded} mA:{mA}')
        elif len(payload_hex)>6: #Esto implica que existen mas datos juntos en la misma trama
            i = i + 1
            k = i
        else:
            k = len(payload_hex) + 1
    return decoded_out

# ***************************************
#   AQUI COMIENZA EL CÓDIGO DE MILESIGHT
# ***************************************
#Function to parse the Payload, typedata is the digits designing the data, size the length of the value to retrieve
def decode_payload(payload, typedata , size):
    
    position=payload.find(typedata)
    
    if position==-1:
        return None

    index =position + len(typedata) # len(obj) 
    data = payload[index:index+(size*2)]
    chunks = [data[i-2:i]for i in range(len(data),0, -2)]
    result=""
    for hex in chunks:
        result+=hex
    return result


#Function to retrieve all the values of the Sensor
def data_payload(payload):
    if len(payload)<56:

        return None

    battery=decode_payload(payload,"0175",1)

    if battery!=None:
        battery=int(battery,16)

    hum = int(decode_payload(payload,"0468",1), 16)
    temp = int(decode_payload(payload,"0367",2), 16)
    
    # (CR:20230613) Modificación para reparar error en la conversión del dato a entero con signo.
    if temp > 0x7FFF:
      temp -= 0x10000

    co2 = int(decode_payload(payload,"077d",2),16)
    activity =int(decode_payload(payload,"056a",2),16)
    illumination=decode_payload(payload,"0665",6)
    lum,vis,infra = illumination[0:4], illumination[4:8] ,illumination[8:]
    lum,vis,infra = int(lum, 16),int(vis,16),int(infra,16)
    tvoc=int(decode_payload(payload,"087d",2),16)
    barometric=int(decode_payload(payload,"0973",2),16)

    #  (CR:20230613) Modificación para reparar error en la conversión del dato a entero con signo.   
    return battery, round(temp*0.1),int(hum*0.5),co2,activity, lum, vis,infra,tvoc,barometric*0.1

# ****************************************
#   AQUI COMIENZA EL CÓDIGO DE EM310-UDL
#   20100110: Decoder Sonar
# ****************************************

def decode_payload_em310udl (payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)
#    print("Payload Raw:",payload_str)
#    print("Payload Hex:",payload_hex.hex())
    decode_out=[]
    k = 0
    i = 0
    while k< len(payload_hex):
        ch_id   = payload_hex[i]
        ch_type = payload_hex[i+1]
        # Battery
        if ch_id == 0x01 and ch_type == 0x75:
            ch_val  = payload_hex[i+2]
            decode_out.append(['bat', ch_val])
            i = i + 3

        # Distance
        ch_id   = payload_hex[i]
        ch_type = payload_hex[i+1]
        if ch_id == 0x03 and ch_type == 0x82:
            ch_val  = payload_hex[i+3] << 8  | payload_hex[i+2] 
            decode_out.append(['val', ch_val])
            i = i + 4
        
        # POSITION
        ch_id   = payload_hex[i]
        ch_type = payload_hex[i+1]
        ch_val  = payload_hex[i+2]
        if ch_id == 0x04 and ch_type == 0x00:
            decode_out.append(['pos', 'tilt']) if ch_val==1 else decode_out.append(['pos', 'normal'])                
        break

    return (decode_out)

# ***************************************
#   AQUI COMIENZA EL DECODER DEL AGRO
# ***************************************
def readInt16BE(bytes):
    ref = (bytes[0] << 8) + bytes[1]
    ref = ref & 0xFFFF
    if ref > 0x7FFF:
        return(ref - 0x10000)
    return (ref)

def readInt32BE(bytes):
    ref = (bytes[0] << 24) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3]
    ref = ref & 0xFFFFFFFF
    if ref > 0x7FFFFFFF:
        return(ref - 0x100000000)
    return (ref)

def readIntU32BE(bytes):
    ref = (bytes[0] << 24) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3]
    return (ref & 0xFFFFFFFF)

def readIntU16BE(bytes):
    ref = (bytes[0] << 8) + bytes[1]
    return (ref & 0xFFFF)

# (CR:20230724) Modificacion para incluir nuevos sensores del Agro
def readIntU8(bytes):
    ref = bytes[0] 
    return (ref & 0xFF)
def readFloatBE(bytes):
    return(B2Fl(readIntU32BE(bytes)))

def B2Fl(b):
    sign = -1 if (b >> 31) else 1
    exp = ((b >> 23) & 0xFF) - 127
    sig = (b & ~(0xFF << 23))

    if exp == 128:
        return sign * (math.nan if sig else math.inf)

    if exp == -127:
        if sig == 0:
            return sign * 0.0
        else:
            exp = -126
            sig /= (1 << 22)
    else:
        sig = (sig | (1 << 23)) / (1 << 23)

    return sign * sig * math.pow(2, exp)

struct_data_sxa1mb13 = {
        'uptime':0,              
        'battery-voltage':0.000,
        'current-adc':0,
        'voltage-adc':0,
        'temperature_1':0.000,
        'temperature_2':0.000,
        'digital-count_4':0,
        'digital-count_3':0,
        'digital-count_2':0,
        'digital-count_1':0,
        'accelerometer-x':0.000,
        'accelerometer-y':0.000,
        'accelerometer-z':0.000,
        'x_offset':0.000,
        'y_offset':0.000
}

# (CR:20230724) Modificacion para incluir nuevos sensores del Agro. Nuevas estructuras de datos.
struct_data_mnla4n101 = {
    'packet-type': '',
    'payload-version': 0,
    'charging-state': 0,
    'fault': 0, 
    'header': 0,
    'uptime': 0,
    'battery-voltage': 0.000,
    'solar-voltage': 0.000,
    'frequency': 0,
    'voltage-adc-1':0 ,
    'voltage-adc-2':0 ,
    'voltage-adc-3':0 ,
    'voltage-adc-4':0 ,
    'digital-count-4': 0,
    'digital-count-3': 0,
    'digital-count-2': 0,
    'digital-count-1': 0,
    'precipitation':0 ,
    'command':0,
    'air-temperature':0.000,
    'relative-humidity':0.000,
    'vapour-pressure-deficit':0.000,
    'barometric-pressure':0.000,
    'saturated-vapour-pressure':0.000,
    'vapour-pressure':0.000,
    'dew-point-temperature':0.000,
    'water-vapour-density':0.000,
    'solar-radiation':0.000
}

struct_data_snla2n204 = {
    'packet-type': '',
    'uptime': 0,
    'battery-voltage':0.000,
    'solar-voltage':0.000,
    'charging-state':0,
    'fault':0,    
    'gnss':0,
    'latitude':0.0000,
    'longitude':0.0000,
    'command':0,
    'soil-moisture-1':0.000,
    'soil-moisture-2':0.000,
    'soil-moisture-3':0.000,
    'soil-moisture-4':0.000,
    'soil-moisture-5':0.000,
    'soil-moisture-6':0.000,
    'soil-moisture-7':0.000,
    'soil-moisture-8':0.000,
    'soil-temperature-1':0.000,
    'soil-temperature-2':0.000,
    'soil-temperature-3':0.000,
    'soil-temperature-4':0.000,
    'soil-temperature-5':0.000,
    'soil-temperature-6':0.000,
    'soil-temperature-7':0.000,
    'soil-temperature-8':0.000,
    'circumference-change':0,
    'sensor-temperature':0.000 
}

# (CR:20231206) Modificacion para incluir sensor de flujo de savia.

struct_data_sxa1mb13_smfx = {
        'packet-type':0,
        'uncorrected-outer':0.000,
        'uncorrected-inner':0.000,
        'corrected-outer':0.000,
        'corrected-inner':0.000,
        'sap-flow-outer':0,
        'sap-flow-inner':0,
        'battery-voltage':0,
        'battery-charge-current':0,
        'internal-battery-temp':0,
        'external-power-supply-present':0,
        'external-power-supply-voltage':0,
        'external-power-supply-current':0,
        'fault':0,
        'header': 0,
        'max-temp-downstream-outer':0.000,
        'max-temp-upstream-outer':0.000,
        'rise-temp-downstream-outer':0,
        'rise-temp-upstream-outer':0,
        'ratio-outer':0,
        'max-temp-downstream-inner':0.000,
        'max-temp-upstream-inner':0.000,
        'rise-temp-downstream-inner':0,
        'rise-temp-upstream-inner':0,
        'ratio-inner':0,
        'pulse-duration':0
}

#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
struct_data_em500_smtc = {
    'channel_id':0,
    'channel_type':0,
    'battery':0,
    'moisture':0.000,
    'ec':0,    
    'temperature':0.000,
    'temperature_change':0.000,
    'temperature_alarm':'',
    'timestamp':0
}

struct_data_em500_udl = {
    'channel_id':0,
    'channel_type':0,
    'battery':0,
    'distance':0.000,
    'timestamp':0
}

struct_data_em500 = {
    'sensor_type':'',
    'channel_id':0,
    'channel_type':0,
    'battery':0,
    'moisture':0.000,
    'ec':0,    
    'temperature':0.000,
    'temperature_change':0.000,
    'temperature_alarm':'',
    'distance':0.000,
    'timestamp':0
}

def decode_payload_sxa1mb13(payload_str):
    #print("Decode SAX1MB13")
    payload_hex = base64.urlsafe_b64decode(payload_str)
    mult = (2.0 / 32678.0) * 1000

    print("Payload Raw Node:",payload_str)
    #print("Payload Hex:",payload_hex.hex())
    #(CR:20230725) Se encontró un error en este punto, Python crea un puntero a la misma estructura, no una copia del diccionario. Con copy() se soluciona.
    deco_out = struct_data_sxa1mb13.copy()
    deco_out['uptime'] = readIntU32BE(payload_hex[0:4])
    deco_out['battery-voltage'] = round( readIntU16BE(payload_hex[4:6])/1000, 3)
    deco_out['current-adc'] = readIntU16BE(payload_hex[6:8])
    deco_out['voltage-adc'] = readIntU32BE(payload_hex[8:12])
    deco_out['temperature_1'] = round(readInt32BE(payload_hex[12:16])/1000,3)
    deco_out['temperature_2'] = round(readInt32BE(payload_hex[16:20])/1000,3)
    deco_out['digital-count_4'] = readInt32BE(payload_hex[20:24])
    deco_out['digital-count_3'] = readInt32BE(payload_hex[24:28])
    deco_out['digital-count_2'] = readInt32BE(payload_hex[28:32])
    deco_out['digital-count_1'] = readInt32BE(payload_hex[32:36])
   
    x= round( readInt16BE(payload_hex[36:38]) * mult ,3)
    y= round( readInt16BE(payload_hex[38:40]) * mult ,3)
    z= round( readInt16BE(payload_hex[40:42]) * mult ,3)
   
    deco_out['accelerometer-x'] = x
    deco_out['accelerometer-y'] = y
    deco_out['accelerometer-z'] = z

    if y>0:
        roll  = abs(math.atan(x/y)*(180/math.pi))
    else:
        roll  = 99

    if z>0:
        pitch = abs(math.atan(x/z)*(180/math.pi))
    else:
        pitch = 99

    x_offset = round( (0 + (90 - roll)) if y < 0 else (0 - (90 - roll)) , 3)
    y_offset = round( (0 + (90 - pitch)) if z < 0 else (0 - (90 - pitch)) , 3)

    deco_out['x_offset'] = x_offset
    deco_out['y_offset'] = y_offset

    return deco_out

# (CR:20231206) Modificacion para incluir sensor de flujo de savia.
def decode_payload_sxa1mb13_sfm1x(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)
    # print("decode_payload_sxa1mb13_sfm1x")
    # print("Payload Raw:",payload_str)
    # print("Payload Hex:",payload_hex.hex())
    
    deco_out = struct_data_sxa1mb13_smfx.copy()
    
    header = readIntU8(payload_hex[0:1])
    
    deco_out['header']= 0x00
    deco_out['packet-type']=0x00

    if header== 0x10:
        # print("Tipo 0x10")
        deco_out['header']= header
        deco_out['uncorrected-outer']= round(readFloatLE(payload_hex[1:5]),3)
        deco_out['uncorrected-inner']= round(readFloatLE(payload_hex[5:9]),3)
        deco_out['corrected-outer']= round(readFloatLE(payload_hex[9:13]),3)
        deco_out['corrected-inner']= round(readFloatLE(payload_hex[13:17]),3)
        deco_out['sap-flow-outer']= round(( readUInt16LE(payload_hex[17:19]) -1000 )/1000,3)
        deco_out['sap-flow-inner']= round(( readUInt16LE(payload_hex[19:21]) -1000 )/1000,3)
        deco_out['battery-voltage']= round(readUInt16LE(payload_hex[21:23])/100,2)
        deco_out['battery-charge-current']= readIntU8(payload_hex[23:24])
        deco_out['internal-battery-temp']= round(readUInt16LE(payload_hex[24:26])/100,2) 
        deco_out['external-power-supply-present']= readIntU8(payload_hex[26:27])
        deco_out['external-power-supply-voltage']= round(readUInt16LE(payload_hex[27:29])/100,2)
        deco_out['external-power-supply-current']= round(readUInt16LE(payload_hex[29:31])/100,2)
        deco_out['fault']= readIntU8(payload_hex[31:32])
    elif header==0x20:
        # print("Tipo 0x20")
        deco_out['header']= header
        deco_out['max-temp-downstream-outer']= round(readFloatLE(payload_hex[1:5]),3)
        deco_out['max-temp-upstream-outer']= round(readFloatLE(payload_hex[5:9]),3)
        deco_out['rise-temp-downstream-outer']= round(readUInt16LE(payload_hex[9:11])/1000,3)
        deco_out['rise-temp-upstream-outer']= round(readUInt16LE(payload_hex[11:13])/1000,3)
        deco_out['ratio-outer']= round(readUInt16LE(payload_hex[13:15])/1000,3)
        deco_out['max-temp-downstream-inner']= round(readFloatLE(payload_hex[15:19]),3)
        deco_out['max-temp-upstream-inner']= round(readFloatLE(payload_hex[19:23]),3)
        deco_out['rise-temp-downstream-inner']= round(readUInt16LE(payload_hex[23:25])/1000,3)
        deco_out['rise-temp-upstream-inner']= round(readUInt16LE(payload_hex[25:27])/1000,3)
        deco_out['ratio-inner']= round(readUInt16LE(payload_hex[27:29])/1000,3)
        deco_out['pulse-duration']= round(readUInt16LE(payload_hex[29:31])/100,2)

    return deco_out

# (CR:20240924) Modificacion para incluir la nueva versión de los Sapflow. Descrito en SFM1x R3-1 LoRa Decoder Firmware R1-0-16 and newer.js
def decode_payload_sxa1mb13_sfm1x_fwR1_0_16(payload_str, fPort):
    payload_hex = base64.urlsafe_b64decode(payload_str)
    print("decode_payload_sxa1mb13_sfm1x_fwR1_0_16")
    # print("Payload Raw:",payload_str)
    # print("Payload Hex:",payload_hex.hex())
    
    deco_out = struct_data_sxa1mb13_smfx.copy()
    
    header = fPort #readIntU8(payload_hex[0:1])

    deco_out['header']= header

    if header== 1:
        # print("Tipo DATA")
        deco_out['uncorrected-outer']= round(readFloatLE(payload_hex[0:4]),3)
        deco_out['uncorrected-inner']= round(readFloatLE(payload_hex[4:8]),3)
        deco_out['corrected-outer']= 0
        deco_out['corrected-inner']= 0
        deco_out['sap-flow-outer']= 0
        deco_out['sap-flow-inner']= 0
        deco_out['battery-voltage']= round(readUInt16LE(payload_hex[8:10])/100,2)
        deco_out['battery-charge-current']= 0
        deco_out['internal-battery-temp']= 0
        deco_out['external-power-supply-present']= 0
        deco_out['external-power-supply-voltage']= 0
        deco_out['external-power-supply-current']= 0
        deco_out['fault']= 0
    else:
        print("Tipo Desconocido")
        print(f"Header:{header}")

    return deco_out

# (CR:20230724) Modificacion para incluir nuevos sensores del Agro. Nuevas estructuras de datos.
def decode_payload_mnla4n101(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)

    print("Payload Raw MFR:",payload_str)
    #print("Payload Hex:",payload_hex.hex())

    charge_fault = readIntU8(payload_hex[0:1])
    header = readIntU8(payload_hex[1:2])
    
    #(CR:20230725) Se encontró un error en este punto, Python crea un puntero a la misma estructura, no una copia del diccionario. Con copy() se soluciona.
    deco_out = struct_data_mnla4n101.copy()
    deco_out['packet-type']='DATA_PACKET'
    deco_out['payload-version']= ((charge_fault & 0xf0) >> 4)
    deco_out['charging-state']= (charge_fault & 1)
    deco_out['fault']= ((charge_fault & 2) >> 1)
    deco_out['header']= int(((header // 16) * 10) + (header % 16))


    if len(payload_hex)<3:
        return(deco_out)

    # print(f"Header:{deco_out['header']}")
    if header==0x10:
        deco_out['uptime']= readIntU32BE(payload_hex[2:6])
        deco_out['battery-voltage'] = round(readIntU16BE(payload_hex[6:8])/1000,3)
        deco_out['solar-voltage']= round(readIntU16BE(payload_hex[8:10])/1000,3)
        deco_out['frequency']= readIntU32BE(payload_hex[10:14])
    elif header==0x20:    
        deco_out['voltage-adc-1']= readIntU32BE(payload_hex[2:6])
        deco_out['voltage-adc-2']= readIntU32BE(payload_hex[6:10])
        deco_out['voltage-adc-3']= readIntU32BE(payload_hex[10:14])
        deco_out['voltage-adc-4']= readIntU32BE(payload_hex[14:18])
    elif header==0x40:
        deco_out['digital-count-4']= readIntU32BE(payload_hex[2:6])
        deco_out['digital-count-3']= readIntU32BE(payload_hex[6:10])
        deco_out['digital-count-2']= readIntU32BE(payload_hex[10:14])
        deco_out['digital-count-1']= readIntU32BE(payload_hex[14:18])
        deco_out['precipitation']= round((deco_out['digital-count-1'] /5),2)
    elif (header==0x80) | (header==0x81) | (header==0x82):
        com = header & 0x0f
        deco_out['command']= com
        # print(f"Com:{com}")
        if com==0:
            deco_out['air-temperature']= round(readFloatBE(payload_hex[2:6]),3)
            deco_out['relative-humidity']= round(readFloatBE(payload_hex[6:10]),3)
            deco_out['vapour-pressure-deficit']= round(readFloatBE(payload_hex[10:14])/1000,3)
            deco_out['barometric-pressure']= round(readFloatBE(payload_hex[14:18]),3)
        elif com==1:
            deco_out['saturated-vapour-pressure']= round(readFloatBE(payload_hex[2:6])/1000,3)
            deco_out['vapour-pressure']= round(readFloatBE(payload_hex[6:10])/1000,3)
            deco_out['dew-point-temperature']= round(readFloatBE(payload_hex[10:14]),3)
            deco_out['water-vapour-density']= round(readFloatBE(payload_hex[14:18]),3)
        elif com==2:
            deco_out['solar-radiation']= round(readFloatBE(payload_hex[2:6]),3)

    return deco_out

# (CR:20230724) Modificacion para incluir nuevos sensores del Agro. Nuevas estructuras de datos.
def decode_payload_snla2n204(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)

    print("Payload Raw SDI:",payload_str)
    #print("Payload Hex:",payload_hex.hex())
    
    #(CR:20230725) Se encontró un error en este punto, Python crea un puntero a la misma estructura, no una copia del diccionario. Con copy() se soluciona.
    deco_out = struct_data_snla2n204.copy()

    deco_out['packet-type']='DATA_PACKET'
    deco_out['uptime']=readIntU32BE(payload_hex[0:4])
    deco_out['battery-voltage']=round(readIntU16BE(payload_hex[4:6])/1000,3)
    deco_out['solar-voltage']=round(readIntU16BE(payload_hex[6:8])/1000,3)

    charge_fault = readIntU8(payload_hex[8:9])

    deco_out['charging-state']= charge_fault & 1
    deco_out['fault']= ( charge_fault & 2 )>>1

    gnss = 1 if ((charge_fault & 4) >> 2) > 0 else 0

    deco_out['gnss']= gnss
    p = 9
    if gnss:
        deco_out['latitude']= round(readInt32BE(payload_hex[9:13])/10000000,6)  
        deco_out['longitude']=round(readInt32BE(payload_hex[13:17])/10000000,6) 
        p = 18

    if len(payload_hex)<18:
        return(deco_out)
    
    com = readIntU8(payload_hex[p:p+1])
    deco_out['command']= com
    if com==0:
        print("soil")
        deco_out['soil-moisture-1']=round(readFloatBE(payload_hex[p+1:p+5]),3)
        deco_out['soil-moisture-2']=round(readFloatBE(payload_hex[p+5:p+9]),3)
        deco_out['soil-moisture-3']=round(readFloatBE(payload_hex[p+9:p+13]),3)
        deco_out['soil-moisture-4']=round(readFloatBE(payload_hex[p+13:p+17]),3)
        deco_out['soil-moisture-5']=round(readFloatBE(payload_hex[p+17:p+21]),3)
        deco_out['soil-moisture-6']=round(readFloatBE(payload_hex[p+21:p+25]),3)
        deco_out['soil-moisture-7']=round(readFloatBE(payload_hex[p+25:p+29]),3)
        deco_out['soil-moisture-8']=round(readFloatBE(payload_hex[p+29:p+33]),3)
    if com==1:
        deco_out['soil-temperature-1']=round(readFloatBE(payload_hex[p+1:p+5]),3)
        deco_out['soil-temperature-2']=round(readFloatBE(payload_hex[p+5:p+9]),3)
        deco_out['soil-temperature-3']=round(readFloatBE(payload_hex[p+9:p+13]),3)
        deco_out['soil-temperature-4']=round(readFloatBE(payload_hex[p+13:p+17]),3)
        deco_out['soil-temperature-5']=round(readFloatBE(payload_hex[p+17:p+21]),3)
        deco_out['soil-temperature-6']=round(readFloatBE(payload_hex[p+21:p+25]),3)
        deco_out['soil-temperature-7']=round(readFloatBE(payload_hex[p+25:p+29]),3)
        deco_out['soil-temperature-8']=round(readFloatBE(payload_hex[p+29:p+33]),3)
    if com==2:
        deco_out['circumference-change']=round(readFloatBE(payload_hex[p+1:p+5]),3)
        deco_out['sensor-temperature']=round(readFloatBE(payload_hex[p+5:p+9]),3)
        
    return deco_out


#(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL

def readTempatureAlarm(type):
    if type == 0:
        return "threshold alarm"
    elif type == 1:
        return "threshold alarm release"
    elif type == 2:
        return "mutation alarm"
    else:
        return "unknown"

def decode_payload_EM500_SMTC(payload_str):

    payload_hex = base64.urlsafe_b64decode(payload_str)
    # print("decode_payload_EM500_SMTC")
    # print("Payload Raw:",payload_str)
    # print("Payload Hex:",payload_hex.hex())
    
    decoded = struct_data_em500_smtc.copy()

    i = 0
    while i < len(payload_hex):
        channel_id = payload_hex[i]
        i += 1
        channel_type = payload_hex[i]
        i += 1

        decoded['channel_id'] = channel_id
        decoded['channel_type'] = channel_type
        
        # BATTERY
        if channel_id == 0x01 and channel_type == 0x75:
            decoded['battery'] = payload_hex[i]
            i += 1
        # TEMPERATURE
        elif channel_id == 0x03 and channel_type == 0x67:
            # ℃
            decoded['temperature'] = readInt16LE(payload_hex[i:i+2]) / 10
            i += 2
        # MOISTURE (old resolution 0.5)
        elif channel_id == 0x04 and channel_type == 0x68:
            decoded['moisture'] = payload_hex[i] / 2
            i += 1
        # MOISTURE (new resolution 0.01)
        elif channel_id == 0x04 and channel_type == 0xca:
            decoded['moisture'] = readUInt16LE(payload_hex[i:i+2]) / 100
            i += 2
        # EC
        elif channel_id == 0x05 and channel_type == 0x7f:
            decoded['ec'] = readUInt16LE(payload_hex[i:i+2])
            i += 2
        # TEMPERATURE CHANGE ALARM
        elif channel_id == 0x83 and channel_type == 0xd7:
            decoded['temperature'] = readInt16LE(payload_hex[i:i+2]) / 10
            decoded['temperature_change'] = readInt16LE(payload_hex[i+2:i+4]) / 10
            decoded['temperature_alarm'] = readTempatureAlarm(payload_hex[i+4])
            i += 5
        # HISTORY
        elif channel_id == 0x20 and channel_type == 0xce:
            decoded['timestamp'] = readUInt32LE(payload_hex[i:i+4])
            decoded['ec'] = readUInt16LE(payload_hex[i+4:i+6])
            decoded['temperature'] = readInt16LE(payload_hex[i+6:i+8]) / 10
            decoded['moisture'] = readUInt16LE(payload_hex[i+8:i+10]) / 100
            i += 10
        else:
            break

    return decoded

def decode_payload_EM500_UDL(payload_str):
    payload_hex = base64.urlsafe_b64decode(payload_str)
    # print("decode_payload_EM500_UDL")
    # print("Payload Raw:",payload_str)
    # print("Payload Hex:",payload_hex.hex())
    
    decoded = struct_data_em500_udl.copy()
    
    i = 0
    while i < len(payload_hex):
        channel_id = payload_hex[i]
        i += 1
        channel_type = payload_hex[i]
        i += 1

        decoded['channel_id'] = channel_id
        decoded['channel_type'] = channel_type

        # BATTERY
        if channel_id == 0x01 and channel_type == 0x75:
            decoded['battery'] = payload_hex[i]
            i += 1
        # DISTANCE
        elif channel_id == 0x03 and channel_type == 0x82:
            decoded['distance'] = readUInt16LE(payload_hex[i:i+2])
            i += 2
        # HISTORY DATA
        elif channel_id == 0x20 and channel_type == 0xce:
            decoded['timestamp'] = readUInt32LE(payload_hex[i:i+4])
            decoded['distance'] = readUInt16LE(payload_hex[i+4:i+6])
            i += 6
        else:
            break

    return decoded

#---------------------------------------------------------------------------------

def insertDataResBuzones2TblResClientes ( db , datosResBuzon, paramConfigBuzones):
    if len(datosResBuzon) > 0:
        cursor =  db.cursor()
        for reg in datosResBuzon:
            sql =" INSERT INTO proyectae.pye_powerbi_res_clientes (`id_bloque` ,`reg_state` ,`fecha_hora_bloque` ,`buzon_id` ,`buzon_name` ,`buzon_diametro`,`faena` ,`cliente` ,`pais` ,`inventario_peso_revisado` ,`porcen_llenado`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            buzon_id = reg[5]
            val = (
                    0,
                    0,
                    reg[4],
                    buzon_id,
                    reg[6],
                    paramConfigBuzones[ paramConfigBuzones['id_buzon'] == buzon_id ]['buzon_diametro' ].tolist()[0],
                    paramConfigBuzones[ paramConfigBuzones['id_buzon'] == buzon_id ]['faena'          ].tolist()[0],
                    paramConfigBuzones[ paramConfigBuzones['id_buzon'] == buzon_id ]['cliente'        ].tolist()[0],
                    paramConfigBuzones[ paramConfigBuzones['id_buzon'] == buzon_id ]['molycop-pais'   ].tolist()[0],
                    reg[25],
                    reg[26],
                )
            cursor.execute(sql, val)
            db.commit()
    return

# YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY

def makeTupla(N):
    tupla  ={'ts_epoch'      : [ 0  ] * N , # ts_epoch
             'altura'        : [ 0  ] * N , # altura
             'id_nodo'       : [ '' ] * N , # id_nodo
             'ts'            : [ '' ] * N , # ts
             'id_datos_nodo' : [ '' ] * N  # ts
            }    
    return(pd.DataFrame(tupla))

def evalTiempoLlegada(tupla, new_val, max_disp):
    # Se evalua la desviación estandar de la muestra y se decide si el nuevo dato esta permitido dentro de este radio.
    # TRUE  = Si esta permitido
    # FALSE = Si no esta permitido

    res = False
    #Solamente selecciono del arreglo los elementos que contienen datos útiles, dejo fuera los valores = 0, porque está vacio.
    N = len(tupla[(tupla['ts_epoch']>0)])
    dat = [0] * (N + 1)
    j = 0

    for d in tupla['ts_epoch'].tolist():
        if( d > 0 ):
            dat[j] = d
            j +=1

    dat[j]  = new_val

    dev_std = np.std(dat)

    if dev_std < max_disp:
        res = True

    return( res )

def marcaDatosDB(db , tupla, estado ):
    try:
        for id in tupla['id_datos_nodo']:
            if id > 0:
                updateRegstatetblDatosNodos(db, id, estado)
    except:
        return        
    return

def updateTupla( tupla, i,  ts, altura, id_datos_nodo , nodo ):
    tupla.iloc[i,tupla.columns.get_loc('ts_epoch')]      = convrtToEpoch(ts)
    tupla.iloc[i,tupla.columns.get_loc('altura')]        = altura
    tupla.iloc[i,tupla.columns.get_loc('ts')]            = ts
    tupla.iloc[i,tupla.columns.get_loc('id_nodo')]       = nodo
    tupla.iloc[i,tupla.columns.get_loc('id_datos_nodo')] = id_datos_nodo
    return

def insertDataDL2tblDatosResBuzones ( db , paramConfigNodos , paramConfigBuzones):
    # 1.- Obtengo los buzones que estan en operación desde el archivo de configuración en drive
    buzones = paramConfigNodos[paramConfigNodos['estado']=='Online'].groupby("id_buzon").groups.keys()
    #print('buzones:', buzones)
    for id_buzon in buzones:
        #2.- Para cada buzon en operación obtengo sus parámetros
        #print('id_buzon:', id_buzon)        
        cantNodosxBuzon = len(paramConfigNodos[(paramConfigNodos['estado']=='Online')&(paramConfigNodos['id_buzon']==id_buzon)])
        maxDispersion = int(paramConfigBuzones[paramConfigBuzones['id_buzon']==id_buzon]['max_dispersion'].tolist()[0])

        #3.- Defino la variable donde se almacenará la información de las medidas tomadas por los Nodos de ese Buzón (es una Tupla)
        tupla_buzon = makeTupla(cantNodosxBuzon)

        #3.1.- i = al indice de llenado de la Tupla con las medidas correctas
        i = 0

        #4.- Busco en la DB todos los registros que llegaron asociados al buzon seleccionado
        dataBuzon = getAllDataBuzon( db , id_buzon)

        #5.- Valido si llegaron datos para ese buzón
        if len(dataBuzon) > 0 :
            #6.- Recorro cada registro de ese buzon en consulta que retorno de la DB
            for reg in dataBuzon:
                nodo = reg[9]
                #6.1.- Valido si la medida de ese nodo existe en la Tuplas
                if nodo in tupla_buzon.values:
                    #6.1.0.- Obtengo el indice asignado al buzon en la matriz
                    j = tupla_buzon.index[ tupla_buzon['id_nodo'] == nodo ].tolist()[0]
                    #6.1.1.- Si ya tengo la info del nodo, se valida su tiempo de llegada y decido si reemplazar la actual o cerrar la medida
                    if evalTiempoLlegada( tupla_buzon , convrtToEpoch(reg[7]) , maxDispersion ):
                       #6.1.2.- Este es el caso donde el tiempo de la medida almacenada es similar a la nueva medida. Por lo tanto, antes de descartar el dato antiguo marco en la DB que ya no será usado para otras comparaciones futuras. Las medidas que se descartaron tienen reg_state=1
                        reg_state = 1
                        updateRegstatetblDatosNodos( db, tupla_buzon.iloc[j,tupla_buzon.columns.get_loc('id_datos_nodo')] , reg_state )
                        updateTupla ( tupla_buzon, j , reg[7] , reg[24], reg[0], nodo )
                    else:
                        #6.1.3.- Este es el caso en que el tiempo de la medida almacenada es muy distinto al de la nueva medida. En este caso cierro la medida almacenada. Las medidas que fueron cerrada tienen reg_state = 2
                        reg_state = 2
                        insertDataDL2TblResBuzones( db, tupla_buzon , id_buzon , cantNodosxBuzon , paramConfigBuzones , paramConfigNodos )
                        marcaDatosDB(db, tupla_buzon, reg_state )
                        #6.1.4.- Ahora reseteo la Tupla para nuevas busquedas
                        tupla_buzon = makeTupla(cantNodosxBuzon)
                        i  = 0
                        #6.1.5.- Finalmente ingreso el nuevo dato a la tupla recien reseteada.
                        updateTupla (tupla_buzon, i , reg[7] , reg[24], reg[0], nodo )
                        i += 1 
                else:
                    #6.2.- Cuando la tupla contiene datos, realizo las validaciones correspondientes
                    if (i < cantNodosxBuzon) & ( i > 0 ):
                        #6.2.1.- Para las siguientes tramas, se valida el tiempo de llegada para decidir si incluirla en la medición o cerrar la medicion
                        if evalTiempoLlegada( tupla_buzon , convrtToEpoch(reg[7]) , maxDispersion ):
                            #6.2.2.- En este caso es un dato nuevo que cumple con la validación, por lo tanto se agrega a la tupla.
                            updateTupla (tupla_buzon, i , reg[7] , reg[24], reg[0], nodo )
                            i = i + 1
                        else:
                            #6.2.3.- En este caso la nueva medida esta fuera del rango de tiempo, por lo tanto se cierra la medida con los datos anteriores. Las medidas que fueron cerrada tienen reg_state = 2
                            reg_state = 2
                            insertDataDL2TblResBuzones( db, tupla_buzon , id_buzon , cantNodosxBuzon, paramConfigBuzones, paramConfigNodos)
                            marcaDatosDB(db, tupla_buzon, reg_state )
                            #6.2.4.- Ahora reseteo la Tupla para nuevas busquedas
                            tupla_buzon = makeTupla(cantNodosxBuzon)
                            i  = 0
                            #6.2.5.- Finalmente ingreso el nuevo dato a la tupla recien reseteada.                        
                            updateTupla (tupla_buzon, i , reg[7] , reg[24], reg[0], nodo )
                            i += 1

                    #6.3.- Condicion inicial. Si la medida no existe, la agrego a la tupla                    
                    if i == 0:
                        #6.2.1.- Condicion inicial, para ver si la tupla esta vacia
                        updateTupla (tupla_buzon, i , reg[7] , reg[24], reg[0], nodo )
                        i = i + 1

                    if  i == cantNodosxBuzon :
                        #6.4.- La tupla esta completa y validada, ahora se cierra la medida y se resetea la tupla.Las medidas que fueron cerrada tienen reg_state = 2
                        reg_state = 2
                        insertDataDL2TblResBuzones( db, tupla_buzon , id_buzon , cantNodosxBuzon ,paramConfigBuzones, paramConfigNodos)
                        marcaDatosDB(db, tupla_buzon, reg_state)
                        #6.4.1.- Ahora reseteo la Tupla para nuevas busquedas
                        tupla_buzon = makeTupla(cantNodosxBuzon)
                        i  = 0

    return

# ---------------------------------------------
# <<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>
# ---------------------------------------------

def calculaAltPromPonderado ( tupla , paramNodos ):
    # Recorro cada fila de la tupla
    alturas_nodos       = [0] * 10
    ponderadores_nodos  = [0] * 10
    #print(tupla)

    for i in tupla.index:
        if len(tupla['id_nodo' ][i]) > 0 : # esto es para el caso en que no se completó la tupla con los datos.
            alturas_nodos[i]      = tupla['altura' ][i]
            ponderadores_nodos[i] = paramNodos[ paramNodos['nombre_nodo'] == tupla['id_nodo'][i] ]['nodo_ponderador'].tolist()[0]

    sum = 0
    r   = 0
    p   = 0

    for k in ponderadores_nodos:
        sum = sum + ( k / 100.0 )
        if k > 0:
            p +=1

    for k in range(0, len(ponderadores_nodos)):
        # Calculo del polinomio incluye los ponderadores de cada sensor. En este caso la fórmula es simple
        r = r + alturas_nodos[k] * (ponderadores_nodos[k] / 100.0)
        #print("alturas_nodos[k]:", alturas_nodos[k] ,"ponderadores_nodos[k]:",ponderadores_nodos[k])

    if sum < 0.99:
        r = r / sum

    return( r , p )

def insertDataDL2TblResBuzones( db, tupla, buzon, cantNodos, paramBuzones, paramNodos ):
    #Nota mental: Que tan necesario es saber la ubicación de un nodo en el buzon??

    NUMMAXDESENSORES = 10

    cursor        =  db.cursor()

    alturas_nodos =  [ 0 ] * NUMMAXDESENSORES    
    i = 0
    for altura in tupla['altura']:
        if (altura>0):
            alturas_nodos[i] = altura
            i = i + 1

    tupla['altura' ][0]
    # Asigno el timestamp a la medida. Se le asigna el TS de a la última que completó la tupla.
    ts_bloque = tupla[tupla['ts_epoch'] == tupla['ts_epoch'].max()]['ts'].tolist()
    
    altura_nodo_promedio_ponderado , porcen_datos_rx_rango = calculaAltPromPonderado ( tupla , paramNodos  )

    porcen_datos_rx_rango = ( porcen_datos_rx_rango / cantNodos ) * 100

    buzon_vol_geometrico= paramBuzones[ paramBuzones['id_buzon'] == buzon]['buzon_vol_geometrico'].tolist()[0]
    buzon_area          = paramBuzones[ paramBuzones['id_buzon'] == buzon]['buzon_area'          ].tolist()[0] 
    inventario_volumen  = buzon_vol_geometrico - (altura_nodo_promedio_ponderado / 100 * buzon_area )

    inventario_densidad = paramBuzones[ paramBuzones['id_buzon'] == buzon]['inventario_densidad' ].tolist()[0]

    inventario_peso     = inventario_densidad * inventario_volumen
    inventario_maxoper  = paramBuzones[ paramBuzones['id_buzon'] == buzon]['inventario_maxoper'  ].tolist()[0]
    
    #inventario_peso_revisado = 0
    if inventario_peso < 0 :
        inventario_peso_revisado = 0
    else:
        inventario_maxrebalse = paramBuzones[ paramBuzones['id_buzon'] == buzon]['inventario_maxrebalse'].tolist()[0]
        if inventario_peso > inventario_maxrebalse:
            inventario_peso_revisado = inventario_maxrebalse
        else:
            inventario_peso_revisado = inventario_peso
    
    try:
        porcen_llenado = inventario_peso_revisado / inventario_maxoper
    except:
        porcen_llenado = 99999

    sql =" INSERT INTO proyectae.pye_powerbi_res_buzones (`reg_state`,`fecha_hora_bloque`,`buzon_id`,`buzon_name`,`buzon_area`,`buzon_vol_geometrico`,`porcen_datos_rx_rango`,`altura_nodo1`,`altura_nodo2`,`altura_nodo3`,`altura_nodo4`,`altura_nodo5`,`altura_nodo6`,`altura_nodo7`,`altura_nodo8`,`altura_nodo9`,`altura_nodo10`,`altura_nodo_promedio_ponderado`,`densidad_bolas`,`inventario_max_operacional`,`inventario_volumen`,`inventario_peso`,`inventario_peso_revisado`,`porcen_llenado`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    val = (
            0,
            ts_bloque[0],
            buzon,
            paramBuzones[ paramBuzones['id_buzon'] == buzon]['buzon_nombre'].tolist()[0],
            buzon_area,
            buzon_vol_geometrico,
            porcen_datos_rx_rango,
            alturas_nodos[0],
            alturas_nodos[1],
            alturas_nodos[2],
            alturas_nodos[3],
            alturas_nodos[4],
            alturas_nodos[5],
            alturas_nodos[6],
            alturas_nodos[7],
            alturas_nodos[8],
            alturas_nodos[9],
            float(altura_nodo_promedio_ponderado),
            inventario_densidad,
            inventario_maxoper,
            float(inventario_volumen),
            float(inventario_peso),
            float(inventario_peso_revisado),
            float(porcen_llenado)
    )
    cursor.execute(sql, val)
    db.commit()

    return

def insertDataDL2tblDatosNodos ( db , paramConfigNodos , paramConfigBuzones):
    # Aqui obtengo los datos para cada nodo informado en el sheet de google. Solo se actualizan esos nodos.

    datosNodosOnline  = paramConfigNodos[paramConfigNodos['estado']=='Online']
    
    for indx, fila in datosNodosOnline.iterrows() : # (CR:20211105) No existe control si existen duplicados
        if len(fila['id_nodo']) > 0:
            idNodo         = fila[ 'id_nodo'  ]
            idBuzon        = fila[ 'id_buzon' ]
            nombreNodo     = fila[ 'nombre_nodo' ]
            #ajusteUtc     = paramConfigBuzones[ 'buzon_ajusteutc' ] [ paramConfigBuzones[ 'id_buzon' ] == idBuzon]
            row_ajusteUtc  = paramConfigBuzones[ paramConfigBuzones[ 'id_buzon' ] == idBuzon ]
            ajusteUtc      = row_ajusteUtc.iloc[0,7]
            #print("ajusteUtc:")
            #print(ajusteUtc)
            #print("++++++++++++++++++++++++++++")

            datosIdNodo = getDataProccessDL( db , idNodo )

            print(idNodo)

            if len(datosIdNodo) > 0 :
                for reg in datosIdNodo:
                    #13 min
                    #14 max                    
                    S2_dist  = reg[21] / 10
                    #print(fila)
                    rango_min = fila[12]
                    rango_max = fila[13]
                    #print('min:', rango_min,' max:',rango_max)
                    reg_state = 3 #Valor fuera de rango
                    if S2_dist >= rango_min and S2_dist <= rango_max:
                        reg_state = 0
                    
                    S2_ang          = fila[5]
                    nodo_alt        = fila[3]
                    S2_pond         = fila[7]
                    nodo_add        = fila[8]
                    nodo_ponderador = fila[9]

                    #Para este calculo se pasa todo a cm y los valores de los porcentajes se dividen por 100, ya que en GSheets están en numero mayor a 1, ej: 50%, etc y no 0.5
                    nodo_altura_process = float( (S2_dist * math.cos(math.radians(S2_ang))-nodo_alt)* S2_pond/100 + nodo_add )

                    sql =" INSERT INTO proyectae.pye_powerbi_datos_nodos (`id_dataup` ,`id_bloque`, `reg_state`, `endDeviceDevEui`, `fecha_hora_lectura_UTC`, `fecha_hora_lectura_local`,`nombre_nodo`, `id_buzon` , `batteryVoltageValue`,`dlDeviceID`,`s1_distancia`,`s1_intentos`,`s2_distancia`,`s2_intentos`,`nodo_altura`,`s1_angulo`,`s2_angulo`,`s1_ponderador`,`s2_ponderador`,`nodo_adicion`,`nodo_ponderador`,`nodo_altura_process`,`distance10p`,`distance25p`,`distance75p`,`distance90p`,`distanceAverage`,`distanceMaximum`,`distanceMedian`,`distanceMinimum`,`distanceMostFrequentValue`,`numberOfSamples`,`protocolVersion`,`totalAcquisitionTime`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val =(
                        reg[1],
                        0,
                        reg_state,
                        reg[7],
                        utc_change_zone( reg[10] , 0         ),
                        utc_change_zone( reg[10] , ajusteUtc ),
                        nombreNodo,
                        idBuzon,
                        reg[13],
                        reg[14],
                        0,
                        0,
                        reg[21],
                        reg[24],
                        fila[3],
                        fila[4],
                        fila[5],
                        fila[6],
                        S2_pond,
                        nodo_add,
                        nodo_ponderador,
                        nodo_altura_process,
                        reg[15],
                        reg[16],
                        reg[17],
                        reg[18],
                        reg[19],
                        reg[20],
                        reg[21],
                        reg[22],
                        reg[23],
                        reg[24],
                        reg[25],
                        reg[26]
                    )
                    cursor =  db.cursor()
                    cursor.execute(sql, val)
                    db.commit()
    return

def googleConnect():
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_cred, scope)
    client = gspread.authorize(creds)
    sh = client.open('Parametros AWS')
    return(sh)

def utc_change_zone(ts_ms, hr_zone):
    fmt  = "%Y-%m-%d %H:%M:%S"
    t = datetime.datetime.fromtimestamp(float(ts_ms)/1000.)
    utc_zone = datetime.timedelta( hours= float(hr_zone) )
    ts_ajus  = t + utc_zone
    return(ts_ajus.strftime(fmt))

def getParamCfgGoogleSheets(name_sheet , sh):

    if name_sheet =='Nodos':
        sh_param   = sh.worksheet("nodos_parametros_b")
        param      = sh_param.get_all_records()
    else:
        sh_param   = sh.worksheet("buzones_parametros_b")
        param      = sh_param.get_all_records()

    return(pd.DataFrame(param))


if __name__ == '__main__':
    #logging.info('Inicio Ciclo.')
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),": Inicio Ciclo")

    appConfig = get_appConf()
    
    db        = dbConnect(sql, appConfig)
    gSheet    = googleConnect()
    print("CR: Google Sheet OK")
    
    allDataDL = DatosDL2Proccess( db )
    decodeDecentlab2InsertDb( allDataDL , db )
    updateRegstateDataup( db, 'Decentlab')
    print("CR: Procesa DL OK")

    allDataMS = DatosMS2Proccess( db )
    decodeMilesight2InsertDb( allDataMS , db )
    updateRegstateDataup( db, 'Milesight')
    print("CR: Procesa MS OK")

    #(CR:20220208) Modificacion para procesar el conversor UC11 con flujometros.
    allDataUC11 = DatosUC112Proccess( db )
    decodeUC112InsertDb  ( allDataUC11 , db )
    #(CR:20220421) Modificacion para incluir requerimiento de Ivan para generar tablas PowerBi.
    decodeUC1152_InsertDb( allDataUC11 , db )
    updateRegstateDataup( db, 'UC11')


    #(CR:20220809) Modificacion para procesar el conversor UC300 con flujometros.
    allDataUC300 = DatosUC300Proccess( db )
    decodeUC300InsertDb  ( allDataUC300 , db )
    decodeUC300Pbi_InsertDb( allDataUC300 , db )
    updateRegstateDataup( db, 'UC300')


    #(CR:20240926) Modificacion para procesar el conversor UC300 a 4-20mA.
    allDataUC300 = DatosUC300Proccess_420mA( db )
    decodeUC3004_20mA_InsertDb( allDataUC300 , db )
    updateRegstateDataup( db, 'UC300_4_20mA')

    #(CR:20230110) Modificacion para procesar el conversor EM310-UDL Ultrasónico.
    allDataEM310 = DatosEM310Proccess( db )
    decodeEM310InsertDb  ( allDataEM310 , db )
    updateRegstateDataup( db, 'EM310')

    print("CR: Procesa EM310 OK")

    #(CR:20230402) Modificacion para procesar el conversor ADLA1L301 del Agro - Lo llamé Agro.
    allDataAgro = DatosAgroProccess_AD_Node( db )
    decodeAgroAdNodeInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_AdNode')

    #(CR:20230724) Modificacion Incluir sensores del Agro MRF y SDI.
    allDataAgro = DatosAgroProccess_Mrf_Node( db )
    decodeAgroMrfNodeInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_MrfNode')

    allDataAgro = DatosAgroProccess_Sdi_Node( db )
    decodeAgroSdiNodeInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_SdiNode')

    print("CR: Procesa AGRO OK")

    #(CR:20231206) Modificacion para incluir sensor de flujo de savia.
    allDataAgro = DatosAgroProccess_SFM1X( db )
    decodeAgroSFM1XInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_SFM1X')

    print("CR: Procesa AGRO SFM1X OK")

    #(CR:20240305) Modificaciones para incluir sensores nuevos EM500_SMTC y EM500_UDL
    allDataAgro = DatosAgroProccess_EM500_SMTC( db )
    decodeAgroEM500smtcInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_EM500_SMTC')

    print("CR: Procesa AGRO EM500_SMTC OK")

    allDataAgro = DatosAgroProccess_EM500_UDL( db )
    decodeAgroEM500udlInsertDb  ( allDataAgro , db )
    updateRegstateDataup( db, 'AGRO_EM500_UDL')

    print("CR: Procesa AGRO EM500_UDL OK")

# ********************************************************************

    paramConfigNodos   = getParamCfgGoogleSheets("Nodos"  , gSheet)
    paramConfigBuzones = getParamCfgGoogleSheets("Buzones", gSheet)
    print("CR: Obtiene ParamConfig GSheets")

    dataDL2PowerBi = getDataProccessDL( db , "")
    insertDataDL2tblPowerBI( dataDL2PowerBi , db)
    print("CR: insert DataDL 2 tblPowerBI")

    insertDataDL2tblDatosNodos ( db , paramConfigNodos , paramConfigBuzones )
    print("CR: insert DataDL 2 tblDatosNodos")

    updateRegstatetblProccess( db, 'Decentlab')
    print("CR: update Regstate tblProccess")

    insertDataDL2tblDatosResBuzones( db , paramConfigNodos , paramConfigBuzones )
    print("CR: insert DataDL 2 tblDatosResBuzones")

    allDataResBuz = getDataResBuzones( db )
    insertDataResBuzones2TblResClientes ( db, allDataResBuz, paramConfigBuzones )
    print("CR: insert Data ResBuzones 2 TblResClientes")
    reg_state = 1
    updateRegstateResBuzones( db, reg_state )
    print("CR: update Regstate ResBuzones")

# ********************************************************************
    dataMS2PowerBi = getDataProccessMS( db )
    insertDataMS2tblPowerBI( dataMS2PowerBi , db)
    updateRegstatetblProccess( db, 'Milesight')


    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"Fin Ciclo")
