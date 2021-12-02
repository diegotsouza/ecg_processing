import asyncio
import math
import os
import signal
import sys
import time
from datetime import datetime,timedelta

import pandas as pd
from bleak import BleakClient
from bleak.uuids import uuid16_dict

## Autor
__author__ = 'souzadt' 
###adapted from https://github.com/pareeknikhil/biofeedback/tree/master/Polar%20Device%20Data%20Stream/ECG

## This is the device MAC ID
ADDRESS = "F7:15:B8:15:A1:F1" 
DEVICE_ID = "6054012C" #device: 21505
##ADDRESS = "" 
##DEVICE_ID = "822CA525" #device: 11429

""""Predefined UUID (Universal Unique Identifier) mapping are based on Heart Rate GATT service Protocol that most
Fitness/Heart Rate device manufacturer follow (Polar H10 in this case) to obtain a specific response input from 
the device acting as an API """

uuid16_dict = {v: k for k, v in uuid16_dict.items()}

## UUID for model number ##
MODEL_NBR_UUID = "0000{0:x}-0000-1000-8000-00805f9b34fb".format(
    uuid16_dict.get("Model Number String")
)

## UUID for manufacturer name ##
MANUFACTURER_NAME_UUID = "0000{0:x}-0000-1000-8000-00805f9b34fb".format(
    uuid16_dict.get("Manufacturer Name String")
)

## UUID for battery level ##
BATTERY_LEVEL_UUID = "0000{0:x}-0000-1000-8000-00805f9b34fb".format(
    uuid16_dict.get("Battery Level")
)

## UUID for connection establsihment with device ##
PMD_SERVICE = "FB005C80-02E7-F387-1CAD-8ACD2D8DF0C8"

## UUID for Request of stream settings ##
PMD_CONTROL = "FB005C81-02E7-F387-1CAD-8ACD2D8DF0C8"

## UUID for Request of start stream ##
PMD_DATA = "FB005C82-02E7-F387-1CAD-8ACD2D8DF0C8"

## UUID for Request of ECG Stream ##
ECG_WRITE = bytearray([0x02, 0x00, 0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0E, 0x00])

## For Plolar H10  sampling frequency ##
ECG_SAMPLING_FREQ = 130

## Define global variables
ecg_session_data = []
ecg_session_time = []
ecg_session_nowtime = []
collect_ecg = True

#date to file
date_time_to_file = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

#outfile name
file_name_ecg = date_time_to_file + "_ID_" + DEVICE_ID

## Keyboard Interrupt Handler
def keyboardInterrupt_handler(signum, frame):
    print("  key board interrupt received...")
    global collect_ecg
    collect_ecg = False

    
## Bit conversion of the Hexadecimal stream
def data_conv(sender, data):
    if data[0] == 0x00:
        timestamp = convert_to_unsigned_long(data, 1, 8)
        step = 3
        samples = data[10:]
        offset = 0
        n=0
        while offset < len(samples):
            ecg = convert_array_to_signed_int(samples, offset, step)
            offset += step
            now_time = datetime.now()
            ecg_session_data.extend([ecg])
            ecg_session_time.extend([timestamp])
            ecg_session_nowtime.extend([str(now_time)])


def convert_array_to_signed_int(data, offset, length):
    return int.from_bytes(
        bytearray(data[offset : offset + length]), byteorder="little", signed=True,
    )


def convert_to_unsigned_long(data, offset, length):
    return int.from_bytes(
        bytearray(data[offset : offset + length]), byteorder="little", signed=False,
    )

## function to save the file
def savetocsv(filePath):
    ## Put data in DataFrame 
    df_ecg = pd.DataFrame(columns=['Time','ECG'])
    df_ecg['Time']=ecg_session_nowtime
    df_ecg['ECG']=ecg_session_data
    
    if not os.path.exists(filePath):
        df_ecg.to_csv(filePath,index=False)
    else:
        df_ecg.to_csv(filePath,index=False, mode= 'a', header=False)
    
    if(ecg_session_data!=[]):
        ecg_session_time.clear()
        ecg_session_nowtime.clear()
        ecg_session_data.clear()

## Aynchronous task to start the data stream for ECG ##
async def run(client, debug=False):

    global collect_ecg
    ## Writing chracterstic description to control point for request of UUID (defined above) ##

    await client.is_connected()
    print("---------Device connected--------------")

    model_number = await client.read_gatt_char(MODEL_NBR_UUID)
    print("Model Number: {0}".format("".join(map(chr, model_number))))

    manufacturer_name = await client.read_gatt_char(MANUFACTURER_NAME_UUID)
    print("Manufacturer Name: {0}".format("".join(map(chr, manufacturer_name))))

    battery_level = await client.read_gatt_char(BATTERY_LEVEL_UUID)
    print("Battery Level: {0}%".format(int(battery_level[0])))
    battery_limit = 30

    att_read = await client.read_gatt_char(PMD_CONTROL)

    await client.write_gatt_char(PMD_CONTROL, ECG_WRITE)

    ## ECG stream started
    await client.start_notify(PMD_DATA, data_conv)

    print("Collecting ECG data...")
    
    while collect_ecg:
        ## Collecting ECG data for 1 second
        await asyncio.sleep(1)
                
        #global ecg_session_data
        filePath = "./data/"+file_name_ecg+".csv"

        if(ecg_session_data==[]):
            print("[warning]:data is None so cant save to",filePath)
        else:
            savetocsv(filePath)
            if(battery_level[0] < battery_limit):
                print("[Warning]:Battery Level: {0}%".format(int(battery_level[0])))
                battery_limit -= 5


    ## Stop the stream once data is collected
    await client.stop_notify(PMD_DATA)
    
    print("Stopping ECG data...")
    print("[CLOSED] application closed.")
    
    sys.exit(0)


async def main():
    #try:
    async with BleakClient(ADDRESS) as client:
        signal.signal(signal.SIGINT, keyboardInterrupt_handler)
        tasks = [
            asyncio.ensure_future(run(client, False)),
        ]

        await asyncio.gather(*tasks)
    #except:
       # pass


if __name__ == "__main__":
    os.environ["PYTHONASYNCIODEBUG"] = str(1)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())