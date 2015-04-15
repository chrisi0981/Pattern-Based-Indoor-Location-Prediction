#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
from datetime import datetime
from datetime import timedelta
from time import time
import math
import random
import sys

db_fields = ['id','room','date','timestamp','week_weekend','weekday','timePeriod','timeIndex','occupancy','roomTemperature','outdoorTemperature','temperatureDifference','temperature_100','temperature_90','temperature_80','temperature_70','temperature_60','temperature_50','look_ahead']

def ReturnWihin1H(look_ahead,accuracy_level):
        
    choice_field_0 = [False]*10
    choice_field_1 = [True]*10
    
    for index in range(accuracy_level):
        choice_field_0[index] = True
        choice_field_1[index] = False
    
    return_in_1h = False
    time_index = -1
    
    for i in xrange(len(look_ahead)):
        if int(look_ahead[i][8]) == 1:
            return_in_1h = True
            time_index = i
            break
            
    if return_in_1h:        
        choice = choice_field_1[random.randint(0,9)]
        
        if choice:            
            time_field = [time_index]*10
            
            for index in range(accuracy_level):
                time_field[index] = random.randint(1,len(look_ahead)-1)
                
            return choice,time_field[random.randint(0,9)]
        else:
            return choice,-1
    else:
        choice = choice_field_0[random.randint(0,9)]
        
        if choice:            
            return choice,random.randint(1,len(look_ahead)-1)
        else:
            return choice,-1     
        

def Oracle_100(room,look_ahead_length):
    
    dbHandler = Database_Handler.Database_Handler("localhost", 3306, "root", "htvgt794jj", "MicroclimateControl")
    
    preferred_temperature = 0
    
    result = dbHandler.select("SELECT avg(roomTemperature) FROM Oracle_Simulator WHERE occupancy = 1")
    
    for row in result:
        preferred_temperature = int(row[0])
        
    
    reduce_temperature = [False,False,False,False,False,False]
    increase_temperature = [False,False,False,False,False,False]
    user_returns = False
    user_in_office = False
    force_change = [False,False,False,False,False,False]
    
    look_ahead = []    
    last_occupancy = -1
    last_temperature = [0,0,0,0,0,0]
    
    result = dbHandler.select("SELECT * FROM Oracle_Simulator WHERE room = '%s'" % (room))
    
    for row in result:
        
        current_occupancy = int(row[8])
        current_room_temperature = float(row[9])
        current_outdoor_temperature = float(row[10])        
        
        if last_occupancy == -1:
            last_occupancy = current_occupancy
            
            for i in range(6):
                last_temperature[i] = current_room_temperature
            
            if current_occupancy == 1:
                user_in_office = True
        else:
            if len(look_ahead) < look_ahead_length:
                look_ahead.append(list(row))                
            else:                
                for i in range(6):                    
                    if reduce_temperature[i]:
                        
                        if user_in_office:
                            last_temperature[i] = last_temperature[i] - 1
                        else:
                            if math.fabs(last_temperature[i] - float(look_ahead[0][10])) > 1 and math.fabs(last_temperature[i] - preferred_temperature) < look_ahead_length: # Need to prevent that room temperature is changed beyond outside temperature to prevent unnecessary heating/cooling
                                last_temperature[i] = last_temperature[i] - 1
                            else:
                                if force_change[i]:
                                    last_temperature[i] = last_temperature[i] - 1
                        
                    if increase_temperature[i]:
                        if user_in_office:
                            last_temperature[i] = last_temperature[i] + 1
                        else:
                            if math.fabs(last_temperature[i] - float(look_ahead[0][10])) > 1 and math.fabs(last_temperature[i] - preferred_temperature) < look_ahead_length:
                                last_temperature[i] = last_temperature[i] + 1
                            else:
                                if force_change[i]:
                                    last_temperature[i] = last_temperature[i] + 1
                    
                    if int(look_ahead[0][8]) == 1:
                        user_in_office = True
                        force_change[i] = False
                        
                        if math.fabs(last_temperature[i]-preferred_temperature) > 1:
                            if last_temperature[i] < preferred_temperature:
                                increase_temperature[i] = True
                                reduce_temperature[i] = False
                            else:
                                increase_temperature[i] = False
                                reduce_temperature[i] = True
                        else:
                            increase_temperature[i] = False
                            reduce_temperature[i] = False
                    else:
                        user_in_office = False                        
                        
                        returns_in_1h,return_time = ReturnWihin1H(look_ahead,i)
                        
                        if returns_in_1h:                            
                            force_change[i] = True
                            
                            if int(math.fabs(last_temperature[i] - preferred_temperature)) >= int(math.ceil(float(return_time)/2)):                                                                
                                if last_temperature[i] < preferred_temperature:
                                    increase_temperature[i] = True
                                    reduce_temperature[i] = False
                                else:
                                    increase_temperature[i] = False
                                    reduce_temperature[i] = True
                            else:
                                if last_temperature[i] < float(look_ahead[0][10]):
                                    increase_temperature[i] = True
                                    reduce_temperature[i] = False
                                else:
                                    increase_temperature[i] = False
                                    reduce_temperature[i] = True
                        else:
                            force_change[i] = False
                            
                            if last_temperature[i] < float(look_ahead[0][10]):
                                increase_temperature[i] = True
                                reduce_temperature[i] = False
                            else:
                                increase_temperature[i] = False
                                reduce_temperature[i] = True
                
                    #Save to DB
                    current_row = look_ahead[0]
                    current_row.append(float(last_temperature[i]))
                
                current_row.append(look_ahead_length)
                dbHandler.insert("Oracle_Simulator_new",db_fields,current_row)
                
                #Push current row on stack
                look_ahead.pop(0)
                look_ahead.append(list(row))

if __name__ == "__main__":
    
    chunk = int(sys.argv[1])
    
    dbHandler = Database_Handler.Database_Handler("localhost", 3306, "root", "htvgt794jj", "MicroclimateControl")
    
    result = dbHandler.select("SELECT DISTINCT room FROM Oracle_Simulator")
    
    count = 0
    
    for row in result:
        if count >= (chunk-1)*70 and count < chunk*70:
            
            look_ahead = 6
            
            while look_ahead < 18:
                start = time()
                Oracle_100(row[0],look_ahead)
                end = time()
                look_ahead = look_ahead + 2
            
            print (end-start)
        count = count + 1