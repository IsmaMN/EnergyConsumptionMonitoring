# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 9:06:17 2021

@author: Ismael
"""
import sys
import os

import serial
import time
import pymodbus
import matplotlib.pyplot as plt
import numpy as np


from pymodbus.pdu import ModbusRequest
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import RPi.GPIO as GPIO
import boto3

#Initialize a serial RTU client instance
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
from datetime import datetime
from datetime import date
from decimal import Decimal


#import sys; 


cnt=1
log_file=open("Registro_eventos.txt", "a+") #Registro de las entradas de sesión 
t_s=datetime.now()
Nota = " Inicia intento de conexión"
log_file.write("%s," %t_s + "%s \r\n" %Nota)

while cnt != 0:
    #Stablish client properties
    client= ModbusClient(method = "rtu", port = "/dev/ttyUSB0", stopbits= 1, 
                         bytesyse = 8, parity = "E", baudrate = 38400)
    #Connect to the serial modbus server
    connection = client.connect()
    if connection == True:
        print("Connected to IEM3255 ENC001 successfully")
        t_s=datetime.now()
        Nota = " Connected to IEM3255 successfully, attemp: "+ str(cnt)
        log_file.write("%s," %t_s + "%s \r\n" %Nota)
        cnt = 0 
    elif cnt < 10:
        print(" Attemping connection: ", cnt)
        time.sleep(3)    
        cnt +=1
        t_s=datetime.now()        
        Nota = " Attemping connection: "+ str(cnt)
        log_file.write("%s," %t_s + "%s \r\n" %Nota)          
    else:
        print("Connection Failed!!")
        t_s=datetime.now()
        Nota = " Connection Failed!"
        log_file.write("%s," %t_s + "%s \r\n" %Nota)                 
        cnt = 0    
log_file.close()


#Funcion para convertir datos de modbus a float32
def list_to_int(l):
    return np.dot(l,np.exp2(np.arange(len(l))))

def conversion(dato):
    #Creamos el arreglo bs que separa individualmente los elementos del string
    bs=np.array(list(dato))

    #el comando b.astype convierte los elemento de la lista a enteros
    bs=bs.astype(int)
    sb = bs[::-1]

    s=sb[-1]
    f=sb[:23]
    e=sb[23:31]

    exponent = list_to_int(e)
    mantissa = list_to_int(f)*np.exp2(-len(f))
    dato_flotante = ((-1)**s)*(1+mantissa)*np.exp2(exponent-127)
    #print 'Valor de variable  medida es', dato_flotante
    return dato_flotante
"""
# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"
# The unique hostname that &IoT; generated for
# this device.

HOST_NAME = "a18qr41ly2kl21-ats.iot.us-west-2.amazonaws.com"
# The relative path to the correct root CA file for &IoT;,
# which you have already saved onto this device.

ROOT_CA = "/certs/AmazonRootCA1.pem"
# The relative path to your private key file that
# &IoT; generated for this device, which you
# have already saved onto this device.

PRIVATE_KEY = "/certs/ff724f3823-private.pem.key"
# The relative path to your certificate file that
# &IoT; generated for this device, which you
# have already saved onto this device.

CERT_FILE = "/certs/ff724f3823-certificate.pem.crt"
# A programmatic shadow handler name prefix.

SHADOW_HANDLER = "ENC001"

# Automatically called whenever the shadow is updated.

def myShadowUpdateCallback(payload, responseStatus, token):
    print()
    print('UPDATE: $aws/things/' + SHADOW_HANDLER +
          '/shadow/update/#')
    print("payload = " + payload)
    print("responseStatus = " + responseStatus)
    print("token = " + token)
    
    # Create, configure, and connect a shadow client
    myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
    myShadowClient.configureEndpoint(HOST_NAME, 8883)
    myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY, CERT_FILE)
    myShadowClient.configureConnectDisconnectTimeout(10)
    myShadowClient.configureMQTTOperationTimeout(5)
    myShadowClient.connect()
    
    #   Create a programmatic representation of the shadow.
    myDeviceShadow = myShadowClient.createShadowHandlerWithName(
        SHADOW_HANDLER, True)
    # To stop running this script, press Ctrl+C.
"""

#Declaracion de los archivos de escritura de datos
    
# Datos para uso trifasico TODO actualizar: 1F2W L-N
    
# Valores instantáneos

# Corrientes
I1_file=open("Corriente_I1", "a+")
I2_file=open("Corriente_I2", "a+")
I3_file=open("Corriente_I3", "a+")
I_media_file=open("Corriente_media", "a+")

#Voltaje
V_L1_L2_file=open("Voljate_L1_L2", "a+")
V_L2_L3_file=open("Voljate_L2_L3", "a+")
V_L3_L1_file=open("Voljate_L3_L1", "a+")
V_L_L_media_file=open("Voljate_L_L_media", "a+")
V_L1_N_file=open("Voljate_L1_N", "a+")
V_L2_N_file=open("Voljate_L2_N", "a+")
V_L3_N_file=open("Voljate_L3_N", "a+")
V_L_N_media_file=open("Voljate_L_N_media", "a+")

#Potencia
PA_1_file=open("Potencia_Activa_F1", "a+")
PA_2_file=open("Potencia_Activa_F2", "a+")
PA_3_file=open("Potencia_Activa_F3", "a+")
PA_total_file=open("Potencia_Activa_total", "a+")
P_Reactiva_file=open("Potencia_Reactiva_total", "a+")
P_Aparente_file=open("Potencia_Aparente_total", "a+")

#Factor de potencia
Factor_Potencia_file=open("Factor_Potencia", "a+")

# Frecuencia
Frecuencia_file=open("Frecuencia", "a+")

# Acumulación de energía
Eimp_file=open("Wh_import.txt", "a+") # Importación de energía activa total, Wh
ERimp_file=open("VARh_import.txt", "a+") # Importación de energía reactiva total
 
def leer_registros (direccion,tamano,ID):
    request = client.read_holding_registers(direccion,tamano,unit=ID)
    respuesta= client.execute(request)
    dato_1=request.registers[0]
    dato_2=request.registers[1]
    dato_1=bin((0x10000 | dato_1))
    dato_2=bin(0x10000 | dato_2)
    dato_1_list=list(dato_1)
    dato_2_list=list(dato_2)
    dato_list = dato_1_list+dato_2_list
    dato_list.pop(0)
    dato_list.pop(0)
    dato_list.pop(0)
    dato_list.pop(17)
    dato_list.pop(17)
    dato_list.pop(17)
    return conversion(dato_list)

#contadorderegistro=0
try :
    while True:
        #Limpiar pantalla
        
        # Estampa de tiempo
        t_s=datetime.now()

        # leer_registros(address, size_of_bytes_to_read, Device_ID)

        # Corrientes
        # Corriente Fase 1
        I1= leer_registros(2999,2,4) # code: 3000
        I1_file.write("%s," %t_s + "%f \r\n" %I1)  
        #Corriente Fase 2
        I2= leer_registros(3001,2,4) # code: 3002
        I2_file.write("%s," %t_s+  "%f \r\n" %I2)
        #Corriente Fase 3       
        I3= leer_registros(3003,2,4)  # code: 3004
        I3_file.write("%s," %t_s+  "%f \r\n" %I3)
        #Corriente media
        I_media= leer_registros(3003,2,4)
        I_media_file.write("%s," %t_s+  "%f \r\n" %I_media)


        # Estampa de tiempo
        t_s=datetime.now()
                # Voltaje  L1-L2
        V_L1_L2= leer_registros(3019,2,4)
        V_L1_L2_file.write("%s," %t_s+  "%f \r\n" %V_L1_L2)
        # Voltaje L2-L3
        V_L2_L3= leer_registros(3021,2,4)
        V_L2_L3_file.write("%s," %t_s+  "%f \r\n" %V_L2_L3)
        # Voltaje L3-L1
        V_L3_L1= leer_registros(3023,2,4)
        V_L3_L1_file.write("%s," %t_s+  "%f \r\n" %V_L3_L1)
        # Voltaje L-L media
        V_L_L= leer_registros(3025,2,4)
        V_L_L_media_file.write("%s," %t_s+  "%f \r\n" %V_L_L)
        #Voltaje L1-N
        V_L1_N= leer_registros(3027,2,4)
        V_L1_N_file.write("%s," %t_s+  "%f \r\n" %V_L1_N)
        #Voltaje L2_N
        V_L2_N= leer_registros(3029,2,4)
        V_L2_N_file.write("%s," %t_s+  "%f \r\n" %V_L2_N)
        #Voltaje L3-N
        V_L3_N=leer_registros(3031,2,4)
        V_L3_N_file.write("%s," %t_s+  "%f \r\n" %V_L3_N)

        #Voltaje L-N media
        V_L_N_media= leer_registros(3035,2,4)
        V_L_N_media_file.write("%s," %t_s+  "%f \r\n" %V_L_N_media)

        # Estampa de tiempo
        t_s=datetime.now()

        #Potencia activa Fase 1
        PA_1= leer_registros(3053,2,4) 
        PA_1_file.write("%s," %t_s+  "%f \r\n" %PA_1)
        #Potencia activa Fase 2
        PA_2= leer_registros(3055,2,4)
        PA_2_file.write("%s," %t_s+  "%f \r\n" %PA_2)
        #Potencia activa Fase 3
        PA_3= leer_registros(3057,2,4)
        PA_3_file.write("%s," %t_s+  "%f \r\n" %PA_3)
        #Potencia activa total
        PA_total= leer_registros(3059,2,4)
        PA_total_file.write("%s," %t_s+  "%f \r\n" %PA_total)
        #Potencia Reactiva total (No aplicable al iEM3150 /iEM3250 /iE; 3350)
        P_Reactiva_total= leer_registros(3067,2,4)
        P_Reactiva_file.write("%s," %t_s+  "%f \r\n" %P_Reactiva_total)

        # Estampa de tiempo
        t_s=datetime.now()

        #Potencia aparente total
        P_Aparente_total= leer_registros(3075,2,4)
        P_Aparente_file.write("%s," %t_s+  "%f \r\n" %P_Aparente_total)


        #Factor de Potencia
        t_s=datetime.now()
        Factor_Potencia= leer_registros(3083,2,4)
        Factor_Potencia_file.write("%s," %t_s+  "%f \r\n" %Factor_Potencia)


        #Frecuencia
        t_s=datetime.now()
        Frecuencia= leer_registros(3109,2,4)
        Frecuencia_file.write("%s," %t_s+  "%f \r\n" %Frecuencia)

        
        #Importación de energía activa total
        t_s=datetime.now()
        Eimp= leer_registros(45099,2,4)
        Eimp_file.write("%s," %t_s+ "%f \r\n" %Eimp)
        
        #Importación de energía reactiva total
        
        ERimp= leer_registros(45103,2,4)
        ERimp_file.write("%s," %t_s+ "%f \r\n" %ERimp)
        #        print(t_s, "   V1", "%.5f" %V1, "  I1", "%.5f" %I1, "  P1", "%.6f" %P1,"  FP", "%.5f" %FP, " Eimport", "%.5f" %Eimp, leer_registros(3203,4,4))
        #print(datetime.now(), "%.4f I1", % I1, "V1", V1, "P1", P1,"Eimp32 ", Eimp)
        #   print (" Corriente I1 ",I1," A", "    Tiempo ", datetime.now())
        print(Frecuencia,V_L1_N,PA_1,Eimp,ERimp,t_s, Eimp, ERimp)
        today = date.today()
        now = datetime.now()
        
        #print(sys.getsizeof(I1)+sys.getsizeof(frec)+sys.getsizeof(V1)+sys.getsizeof(P1)+sys.getsizeof(FP)+sys.getsizeof(Stot)+sys.getsizeof(Qtot)+sys.getsizeof(Eimp)+sys.getsizeof(ERimp)+sys.getsizeof(t_s)) # pasas como parámetro la variable que deseas en bytes

        """
        # print("\n-------------Envío a Dynamo----------------------\n")
        
        #s3= boto3.client('s3')
        #s3.upload_file('s3_transfer.txt', 'revrite-stremeing-dev', 's3_scrip1.txt')
        dynamodb = boto3.resource('dynamodb', 'us-west-2')
        print (dynamodb)
        
        dynamoTable = dynamodb.Table('ENC001')
        dynamoTable.put_item(
            Item= {
                'I1':("%f"%I1),
                'I2':("%f"%I2),
                'I3':("%f"%I3),
                'I_media':("%f"%I_media),
                'V_L1_L2':( "%f"%V_L1_L2),
                'V_L2_L3':( "%f"%V_L2_L3),
                'V_L3_L1':( "%f"%V_L3_L1),
                'V_L_L':( "%f"%V_L_L),
                'V_L1_N':( "%f"%V_L1_N),
                'V_L2_N':( "%f"%V_L2_N),
                'V_L3_N':( "%f"%V_L3_N),
                'V_L_N_media':( "%f"%V_L_N_media),
                'PA_1':("%f"%PA_1),
                'PA_2':("%f"%PA_2),
                'PA_3':("%f"%PA_3),
                'PA_total':("%f"%PA_total),
                'P_Reactiva_total':("%f"%P_Reactiva_total),
                'P_Aparente_total':("%f"%P_Aparente_total),
                'Factor_Potencia':("%f"%Factor_Potencia),
                'Frecuencia':("%f"%Frecuencia),
                'ImpEneActTot':("%f"%Eimp),
                'ImpEneReacTot':("%f"%ERimp),
                'Time':("%s"%t_s),
                'year':("{}".format(today.year)),
                'month':("{}".format(today.month)),
                'day':("{}".format(today.day)),
                'time': ("{}".format(now.hour)),
                'minutes': ("{}".format(now.minute))
                }
            )
    """
        #contadorderegistro=contadorderegistro+1
        #print("%i"%contadorderegistro)
        #Tiempo de espera
        time.sleep(60)

except KeyboardInterrupt:
    t_s=datetime.now()   
    log_file=open("Registro_eventos.txt", "a+")       
    Nota = " Se detiene comunicación"        
    log_file.write("%s," %t_s + "%s \r\n" %Nota)
    #Cerramos los archivos de datos:
    I1_file.close()
    I2_file.close()
    I3_file.close()
    I_media_file.close()
    V_L1_L2_file.close()
    V_L2_L3_file.close()
    V_L3_L1_file.close()
    V_L_L_media_file.close()
    V_L1_N_file.close()
    V_L2_N_file.close()
    V_L3_N_file.close()
    V_L_N_media_file.close()
    PA_1_file.close()
    PA_2_file.close()
    PA_3_file.close()
    PA_total_file.close()
    P_Reactiva_file.close()
    P_Aparente_file.close()
    Factor_Potencia_file.close()
    Frecuencia_file.close()
    Eimp_file.close()
    ERimp_file.close()
    log_file.close()
    #Closes the underlying socket connection
    client.close()
    print("Communitacion stopped")
     
