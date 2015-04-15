#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys
from multiprocessing import Process
from multiprocessing import Pool
import os
import thread
import threading

import random
import itertools

import time
import datetime
from pytz import timezone

import numpy
from collections import Counter
from operator import itemgetter
import pickle
import gc
import pp

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Prepare_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "GILP")
	dbHandler_data = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction")
	
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','singular_pattern_id']
	
	rooms = []
	
	result = dbHandler.select("SELECT DISTINCT room_name FROM Room_IDs")
	
	for row in result:
		rooms.append("room_%s_sensorData" % (row[0]))
		
	room_id = 0		
	
	for room in rooms:
		room_id = room_id + 1
		
		#dbHandler_data.truncateTable("Singular_Pattern_Base")
		#dbHandler_data.update("INSERT INTO Singular_Pattern_Base SELECT * FROM Pattern_Base.Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i" % (room_id))
		
		sp,sp_duration = Get_Singular_Patterns(room_id)
		
		# Timestamp,Date,Time,Time Index,DoW,Occupancy
		result = dbHandler.select("SELECT timestamp,FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') AS date,FROM_UNIXTIME(timestamp,'%%H:%%i:%%s') as time,FLOOR((HOUR(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s'))*60+MINUTE(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s')))/5) AS time_index,WEEKDAY(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')) AS dow,OCC_Sensor,DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),MONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),CEIL(DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d'))/7) FROM %s WHERE (OCC_Sensor = 'Occupied' OR OCC_Sensor = 'Unoccupied') AND FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') > '2011-12-31' ORDER BY timestamp" % (room))
		
		data = []
				
		for row in result:
			data.append(row)
		
		if len(data) > 0:
			print room_id,room
			
			new_data = []
			new_test_data = []
			
			last_occupancy = ""
			transition_time = -1
			last_duration = 0			
			
			for data_point in data:
				
				if last_occupancy == "":
					last_occupancy = Get_Occupancy(data_point[5])
					transition_time = int(data_point[3])
					
				if last_occupancy != Get_Occupancy(data_point[5]):
					
					duration = (int(data_point[3])-transition_time)%288
					season = 0
					week_weekend = 0
					
					if int(data_point[4]) <= 4:
						week_weekend = 1
					
					if int(data_point[7]) < 4: 
						season = 0 # Winter
						
					if int(data_point[7]) >= 4 and int(data_point[7]) < 7:
						season = 1 # Spring
						
					if int(data_point[7]) >= 7 and int(data_point[7]) < 10:
						season = 2 # Summer
						
					if int(data_point[7]) >= 10:
						season = 3 # Fall
					
					for current_time in range((int(data_point[3])-transition_time)%288):
					
						tmp = []
						tmp.append(room_id)
						tmp.append(float(data_point[0])-(duration - current_time)*5*60)
						tmp.append(datetime.datetime.fromtimestamp(int(float(data_point[0])-(duration - current_time)*5*60)).strftime('%Y-%m-%d'))
						tmp.append(datetime.datetime.fromtimestamp(int(float(data_point[0])-(duration - current_time)*5*60)).strftime('%H:%M:%S'))						
						tmp.append(int(transition_time+current_time)%288)
						
						if int(data_point[4]) > 4:
							tmp.append(2)
						else:
							tmp.append(5)
							
						tmp.append(int(data_point[4]))
						tmp.append(last_occupancy)
						tmp.append(current_time)
						tmp.append(transition_time)
						tmp.append(last_duration)
												
						singular_patterns = []
						
						activities = ["Transition-To","Transition-From","First Arrival","Last Departure","Duration"]
						
						time_index = int(tmp[4])
						
						for poss_location in range(2):
							for activity in range(4):
								temporal_offset = 0
								sp_tmp = []
								
								# Daily
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][0]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][0]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][0]
								else:
									sp_tmp = sp_tmp + sp[poss_location][activity][upper][0]
									
								# Day of Week
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][3][int(tmp[6])]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][3][int(tmp[6])]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][3][int(tmp[6])]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][3][int(tmp[6])]
									
								# Day of Month							
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][5][int(data_point[6])-1]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][5][int(data_point[6])-1]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][5][int(data_point[6])-1]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][5][int(data_point[6])-1]
									
								# Season
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][2][season]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][2][season]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][2][season]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][2][season]
									
								# Week vs. Weekend
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][1][week_weekend]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][1][week_weekend]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][1][week_weekend]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][1][week_weekend]
									
								# Number in Month
								lower = -1
								upper = -1
								
								for k in range(time_index+1):
									if len(sp[poss_location][activity][time_index-k][4][tmp[6]][int(data_point[8])]) > 0:
										lower = time_index-k
										break
										
								for k in range(287-time_index):
									if len(sp[poss_location][activity][time_index+(k+1)][4][tmp[6]][int(data_point[8])]) > 0:
										upper = time_index+k+1
										break
								
								if (time_index-lower) <= (upper-time_index) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][4][tmp[6]][int(data_point[8])]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][4][tmp[6]][int(data_point[8])]
									
								singular_patterns = singular_patterns + sp_tmp
						
						tmp.append(','.join(map(str,singular_patterns)))
						
						new_data.append(tmp)					
					
					last_occupancy = Get_Occupancy(data_point[5])
					last_duration = duration
					transition_time = int(data_point[3])
					
				
				if len(new_data) > 5000:
					dbHandler_data.insert_bulk("GHC_Test_Data",data_fields,new_data)
					new_data = []
					
			dbHandler_data.insert_bulk("GHC_Test_Data",data_fields,new_data)
		
		
def Get_Occupancy(occupancy):
	
	if occupancy == 'Occupied':
		return 1
	
	if occupancy == 'Unoccupied':
		return 0

def Get_Singular_Patterns(user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
		
		
	result = dbHandler.select("SELECT max(time) FROM Singular_Pattern_Base WHERE activity = 'Duration' AND user_id = %i" % (user))
		
	max_duration = 0
	
	for row in result:
		if row[0] != None:
			max_duration = int(row[0])
	
	singular_patterns = []
	
	for j in range(2):
		activities = []
	
		for j in range(4):
			time = []
		
			for j in range(288):
				specificity = []

				specificity.append([]) # Daily
				
				tmp = []
				tmp.append([])
				tmp.append([])
				specificity.append(tmp) # Week vs. Weekend
				
				tmp = []
				
				for k in range(4):
					tmp.append([])
					
				specificity.append(tmp) # Season
				
				tmp = []
				
				for k in range(7):
					tmp.append([])
					
				specificity.append(tmp) # Weekday
				
				tmp = []
				
				for k in range(7):
					tmp_1 = []
					
					for l in range(7):
						tmp_1.append([])
					
					tmp.append(tmp_1)
					
				specificity.append(tmp) # Number in Month
				
				tmp = []
				
				for k in range(31):
					tmp.append([])
					
				specificity.append(tmp) # Day of Month
				
				time.append(specificity)
			
			activities.append(time)
		
		singular_patterns.append(activities)
			
		
	singular_patterns_duration = []
		
	locations = []
	
	for j in range(2):
		time = []
	
		for j in range(max_duration):
			specificity = []

			specificity.append([]) # Daily
			
			tmp = []
			tmp.append([])
			tmp.append([])
			specificity.append(tmp) # Week vs. Weekend
			
			tmp = []
			
			for k in range(4):
				tmp.append([])
				
			specificity.append(tmp) # Season
			
			tmp = []
			
			for k in range(7):
				tmp.append([])
				
			specificity.append(tmp) # Weekday
			
			tmp = []
			
			for k in range(7):
				tmp_1 = []
				
				for l in range(7):
					tmp_1.append([])
				
				tmp.append(tmp_1)
				
			specificity.append(tmp) # Number in Month
			
			tmp = []
			
			for k in range(31):
				tmp.append([])
				
			specificity.append(tmp) # Day of Month
			
			time.append(specificity)
		
		singular_patterns_duration.append(time)
		
		
	result = dbHandler.select("SELECT * FROM Singular_Pattern_Base WHERE user_id = %i AND LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2" % (user))
	
	"""	
	Singular Pattern Schema:	
		Location -> Activity -> Time -> Specificity -> Pattern ID
		
	Singular Pattern Duration Schema:	
		Location -> Duration -> Specificity -> Pattern ID
	"""
	
	for row in result:
		
		if row[3] != 'Duration':
			if int(row[14]) == 0:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])].append(int(row[2])) # Daily
				
			if int(row[14]) == 1:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[12])].append(int(row[2])) # Week vs. Weekend
				
			if int(row[14]) == 2:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[11])].append(int(row[2])) # Season
				
			if int(row[14]) == 3:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[8])].append(int(row[2])) # Day of Week
				
			if int(row[14]) == 4:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[8])][int(row[13])-1].append(int(row[2])) # Number in Month
				
			if int(row[14]) == 5:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[7])-1].append(int(row[2])) # Day of Month
		else:			
			if int(row[14]) == 0:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])].append(int(row[2])) # Daily
				
			if int(row[14]) == 1:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[12])].append(int(row[2])) # Week vs. Weekend
				
			if int(row[14]) == 2:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[11])].append(int(row[2])) # Season
				
			if int(row[14]) == 3:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[8])].append(int(row[2])) # Day of Week
				
			if int(row[14]) == 4:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[8])][int(row[13])-1].append(int(row[2])) # Number in Month
				
			if int(row[14]) == 5:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[7])-1].append(int(row[2])) # Day of Month
		
			
	return singular_patterns,singular_patterns_duration

def Get_Activity_Code(activity):
	
	if activity == 'Transition-To':
		return 0
	
	if activity == 'Transition-From':
		return 1		
	
	if activity == 'First Arrival':
		return 2
	
	if activity == 'Last Departure':
		return 3

def Gradient_Descent(user,pattern_length,look_ahead,epsilon,data):
	
	pass

def Predict_Occupancy(user,pattern_length,look_ahead,ensemble,processes):
	
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','singular_pattern_id','occupancy_30','prob_30','pattern_30','pattern_length']
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction")
	
	"""
	
	Load extracted patterns!
	
	"""
	dbHandler.truncateTable("Singular_Pattern_Base")
	dbHandler.update("INSERT INTO Singular_Pattern_Base SELECT * FROM Pattern_Base.Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i" % (user))
		
	max_sp_id = -1
	
	result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		max_sp_id = int(row[0])
		
	singular_patterns_prob = numpy.zeros(max_sp_id,float)
	singular_patterns_occ = numpy.zeros(max_sp_id,float)
	
	result = dbHandler.select("SELECT pattern_id,probability,location FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		singular_patterns_prob[int(row[0])-1] = float(row[1])
		singular_patterns_occ[int(row[0])-1] = int(row[2])
	
	# Specificity weight: 0.14285714
	occupied = numpy.nonzero(singular_patterns_occ)[0]
	not_occupied = numpy.where(numpy.array(singular_patterns_occ) == 0)[0]
		
	temporal_condition = [[] for k in range(7)]
	tc_normalization = [1,2,3,7,14,21,31]
	
	result = dbHandler.select("SELECT pattern_members,probability,specificity,TRIM(TRAILING ',' FROM SUBSTRING_INDEX(pattern_members, ',',-2)),count FROM Pattern_Base.Pattern_Base_%i WHERE pattern_length = %i" % (user,pattern_length))
	
	valid_patterns = {}
	
	for row in result:
		
		try:
			current_values = valid_patterns[row[0]]
			current_values[int(float(row[2])/0.14285714)-1] = float(row[1])
			tmp[7] = int(row[3])
			temporal_condition[int(float(row[2])/0.14285714)-1].append(float(row[4])/float(tc_normalization[int(float(row[2])/0.14285714)-1]))
			valid_patterns[row[0]] = current_values		
		except KeyError, e:
			tmp = numpy.zeros(9,float)
			tmp[int(float(row[2])/0.14285714)-1] = float(row[1])
			tmp[7] = int(row[3])
			temporal_condition[int(float(row[2])/0.14285714)-1].append(float(row[4])/float(tc_normalization[int(float(row[2])/0.14285714)-1]))
			valid_patterns[row[0]] = tmp
	
	tc_ranking = [(numpy.median(temporal_condition[k]),k,len(temporal_condition[k])) for k in range(7)]
	
	
	result = dbHandler.select("SELECT user_id,timestamp,date,time,time_index,week_weekend,current_dow,current_location,current_duration,transition_time,last_duration,singular_pattern_id,WEEK(date),MONTH(date),WEEKDAY(date) FROM GHC_Test_Data WHERE user_id = %i ORDER BY timestamp" % (user))
	
	window = []	
	
	patterns = []
	dates = []
	days = []
	weeks = []
	months = []
	timestamps = []
	
	result_data = []
	
	weights = [1,1,1,1,1,1,1]
	
	pool = Pool(processes)
	
	last_date = ""
	sd_comb_old=pd_comb_old=p2d_comb_old=sw_comb_old=pw_comb_old=p2w_comb_old=sm_comb_old = []
	sd_unique_patterns=pd_unique_patterns=p2d_unique_patterns=sw_unique_patterns=pw_unique_patterns=p2w_unique_patterns=sm_unique_patterns = []
	sd_old_patterns=pd_old_patterns=p2d_old_patterns=sw_old_patterns=pw_old_patterns=p2w_old_patterns=sm_old_patterns = []
	
	temporal_complexity = [[] for k in range(4)]
	
	overall_start = time.time()
	
	for row in result:				
		
		if len(result_data) > 5000:
			dbHandler.insert_bulk("GHC_Test_Result",data_fields,result_data)
			result_data = []
		
		if int(row[1]) <= 1330491452 and int(row[1]) >= 1328072525: # 1330491452 == '2011-02-28' 1328072525 == '2011-02-01
			if row[11] != '':
				data_point_patterns = map(int,row[11].split(","))
				
				if int(row[7]) == 1:#"%i" % (k)
					patterns.append(["%i" % (k) for k in data_point_patterns if k in occupied])
				else:
					patterns.append(["%i" % (k) for k in data_point_patterns if k in not_occupied])
				
				date = row[2].split('-')
				dates.append(datetime.date(int(date[0]),int(date[1]),int(date[2])))
				days.append(int(row[14]))
				weeks.append(int(row[12]))
				months.append(int(row[13]))
				timestamps.append(int(row[1]))
		else:
			if int(row[1]) > 1330491452 and int(row[1]) < 1330664225:
				
				if len(window) < look_ahead:
					window.append(row)
				else:					
					target_patterns = row[11].split(',')
					
					if pattern_length == 1:
						
						max_prob = -1
						occ = -1
						predicted_pattern = 0
						
						for target in target_patterns:
							if target != '':
								if singular_patterns_prob[int(target)-1] > max_prob:
									max_prob = singular_patterns_prob[int(target)-1]
									occ = singular_patterns_occ[int(target)-1]
									predicted_pattern = int(target)
								
						row = list(row)
						row.append(occ)
						row.append(max_prob)
						row.append(predicted_pattern)
						row.append(pattern_length)
						
						result_data.append(row)
					else:
						if window[-1][11] != '':
							data_point_patterns = map(int,window[-1][11].split(","))
							
							if last_date != row[2]:
								sd_comb_old=pd_comb_old=p2d_comb_old=sw_comb_old=pw_comb_old=p2w_comb_old=sm_comb_old = []
								sd_unique_patterns=pd_unique_patterns=p2d_unique_patterns=sw_unique_patterns=pw_unique_patterns=p2w_unique_patterns=sm_unique_patterns = []
								sd_old_patterns=pd_old_patterns=p2d_old_patterns=sw_old_patterns=pw_old_patterns=p2w_old_patterns=sm_old_patterns = []
								
							start = time.time()
							
							date = window[0][2].split('-')
							date = datetime.date(int(date[0]),int(date[1]),int(date[2]))
							same_day_indices = numpy.where(numpy.array(dates) == date)[0]
							previous_day_indicies = list(same_day_indices) + [k for k in numpy.where(numpy.array(dates) == (date + datetime.timedelta(days=-1)))[0]]
							previous_2day_indicies = previous_day_indicies + [k for k in numpy.where(numpy.array(dates) == (date + datetime.timedelta(days=-2)))[0]]
							
							same_week_indices = numpy.where(numpy.array(weeks) == window[0][12])[0]
							previous_week_indicies = list(same_week_indices) + [k for k in numpy.where(numpy.array(weeks) == (window[0][12]-1)%53)[0]]
							previous_2week_indicies = previous_week_indicies + [k for k in numpy.where(numpy.array(weeks) == (window[0][12]-2)%53)[0]]
							
							same_month_indices = numpy.where(numpy.array(months) == window[0][13])[0]
							
							temporal_complexity[0].append(time.time() - start)
							
							start = time.time()
							
							#0
							sd_comb,sd_comb_old,sd_unique_patterns,sd_old_patterns = Create_Pattern_Combinations(patterns,same_day_indices,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)																													
							#1							
							pd_comb,pd_comb_old,pd_unique_patterns,pd_old_patterns = Create_Pattern_Combinations(patterns,previous_day_indicies,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)													
							#2							
							p2d_comb,p2d_comb_old,p2d_unique_patterns,p2d_old_patterns = Create_Pattern_Combinations(patterns,previous_2day_indicies,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)														
							#3							
							sw_comb,sw_comb_old,sw_unique_patterns,sw_old_patterns = Create_Pattern_Combinations(patterns,same_week_indices,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)														
							#4
							pool_result = None
							#pw_comb,pw_comb_old,pw_unique_patterns,pw_old_patterns = Create_Pattern_Combinations(patterns,previous_week_indicies,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)
							pool_result = pool.apply_async(Create_Pattern_Combinations, (patterns,previous_week_indicies,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns,))
							pw_comb,pw_comb_old,pw_unique_patterns,pw_old_patterns = pool_result.get()
							#5							
							p2w_comb,p2w_comb_old,p2w_unique_patterns,p2w_old_patterns = Create_Pattern_Combinations(patterns,previous_2week_indicies,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)							
							#6							
							sm_comb,sm_comb_old,sm_unique_patterns,sm_old_patterns = Create_Pattern_Combinations(patterns,same_month_indices,data_point_patterns,pattern_length,sd_comb_old,sd_unique_patterns,sd_old_patterns,valid_patterns)							
							
							temporal_complexity[1].append(time.time() - start)
							
							start = time.time()
							
							sd_prob = [(k,valid_patterns[k][0]*weights[0],0,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][0]) for k in sd_comb if valid_patterns[k][0] != 0]
							pd_prob = [(k,valid_patterns[k][1]*weights[1],1,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][1]) for k in pd_comb if valid_patterns[k][1] != 0]
							p2d_prob = [(k,valid_patterns[k][2]*weights[2],2,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][2]) for k in p2d_comb if valid_patterns[k][2] != 0]
							
							sw_prob = [(k,valid_patterns[k][3]*weights[3],3,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][3]) for k in sw_comb if valid_patterns[k][3] != 0]
							pw_prob = [(k,valid_patterns[k][4]*weights[4],4,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][4]) for k in pw_comb if valid_patterns[k][4] != 0]
							p2w_prob = [(k,valid_patterns[k][5]*weights[5],5,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][5]) for k in p2w_comb if valid_patterns[k][5] != 0]
							
							sm_prob = [(k,valid_patterns[k][6]*weights[6],6,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][6]) for k in sm_comb if valid_patterns[k][6] != 0]
							
							temporal_complexity[2].append(time.time() - start)
							
							if int(row[1]) < 1333321210:
								start = time.time()
								
								weight_array = []
								
								target_probabilities = []
								cut_off = 0.01
								
								targets = [0,1]
								
								for current_target in targets:
									
									target_tmp = []
								
									# Same Day Probability
									target_tmp.append([sd_prob[k][5] for k in range(len(sd_prob)) if sd_prob[k][4] == current_target and sd_prob[k][5] >= cut_off])									
									
									# One Adjacent Day Probability
									target_tmp.append([pd_prob[k][5] for k in range(len(pd_prob)) if pd_prob[k][4] == current_target and pd_prob[k][5] >= cut_off])									
									
									# Two Adjacent Day Probability
									target_tmp.append([p2d_prob[k][5] for k in range(len(p2d_prob)) if p2d_prob[k][4] == current_target and p2d_prob[k][5] >= cut_off])									
									
									# Same Week Probability
									target_tmp.append([sw_prob[k][5] for k in range(len(sw_prob)) if sw_prob[k][4] == current_target and sw_prob[k][5] >= cut_off])	
									
									# One Adjacent Week Probability
									target_tmp.append([pw_prob[k][5] for k in range(len(pw_prob)) if pw_prob[k][4] == current_target and pw_prob[k][5] >= cut_off])									
									
									# Two Adjacent Week Probability
									target_tmp.append([p2w_prob[k][5] for k in range(len(p2w_prob)) if p2w_prob[k][4] == current_target and p2w_prob[k][5] >= cut_off])								
									
									# Same Month Probability
									target_tmp.append([sm_prob[k][5] for k in range(len(sm_prob)) if sm_prob[k][4] == current_target and sm_prob[k][5] >= cut_off])
									
									target_probabilities.append(target_tmp)
								
								mean_probs = []
								meadian_probs = []
								max_probs = []
								
								for k in range(len(targets)):
									
									mean_probs.append([numpy.mean(l) for l in target_probabilities[k]])
									meadian_probs.append([numpy.median(l) for l in target_probabilities[k]])
									max_probs.append([numpy.max(l) for l in target_probabilities[k] if len(l) > 0])
									
								print mean_probs
								print map(itemgetter(3),mean_probs)
								
								temporal_complexity[3].append(time.time() - start)
							
							"""
							if len(highest_prob) >= 5:
								print highest_prob
							else:
								print highest_prob
							"""
							
							last_date = row[2]	
							
					if window[0][11] != '':
						data_point_patterns = map(int,window[0][11].split(","))
						
						if int(window[0][7]) == 1:
							patterns.append(["%i" % (k) for k in data_point_patterns if k in occupied])
						else:
							patterns.append(["%i" % (k) for k in data_point_patterns if k in not_occupied])
						
						date = window[0][2].split('-')
						dates.append(datetime.date(int(date[0]),int(date[1]),int(date[2])))
						days.append(int(window[0][14]))
						weeks.append(int(window[0][12]))
						months.append(int(window[0][13]))
						timestamps.append(int(row[1]))
					
					window.pop(0)
					window.append(row)										
			
	print time.time() - overall_start
	print [(sum(l),numpy.mean(l)) for l in temporal_complexity]

	
def Create_Pattern_Combinations(patterns,indices,target_patterns,pattern_length,list_pl_2,previous_unique_patterns,previous_patterns,valid_patterns):
	
	if len(indices) >= pattern_length-1:
		if list_pl_2 == []:
			final_patterns = [patterns[k] for k in indices]		
			unique_patterns = set([i for k in final_patterns for i in k])
			list_pl_1 = [list(k) for k in itertools.permutations(unique_patterns,pattern_length-1)]
			list_pl_2 = [list(k) for k in itertools.permutations(unique_patterns,pattern_length-2)]
			
			comb = [",%s,%s," % (','.join(list(k)),i) for k in list_pl_1 for i in target_patterns]			
			comb = [k for k in comb if k in valid_patterns]			
			
			return comb,list_pl_2,unique_patterns,list_pl_1
		else:
			
			final_patterns = [patterns[k] for k in indices]
			unique_patterns = set([i for k in final_patterns for i in k if i not in previous_unique_patterns])			
			
			new_comb = []
			comb = []
			comb_long = []
			
			start = time.time()
			for k in list_pl_2:
				for i in range(len(k)+1):
					for j in unique_patterns:
						l = list(k)
						l.insert(i,j)						
						
						comb_long.append(l)
						
						for m in target_patterns:
							comb.append(",%s,%s," % (','.join(l),m))
							
						if i < len(k):
							n = list(k)
							n[i] = j
							
							if not n in new_comb:
								new_comb.append(n)
													
			
			previous_comb = [",%s,%s," % (','.join(list(k)),i) for k in previous_patterns for i in target_patterns]			
			comb = comb + previous_comb			
			comb = [k for k in comb if k in valid_patterns]			
			#print "Pattern Creation: ",time.time() - start
			
			return comb,list_pl_2+new_comb,set(list(previous_unique_patterns)+list(unique_patterns)),previous_patterns + comb_long
	else:
		return [],[],[],[]

# ================================= End of Support Methods =================================

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	user = int(sys.argv[3])
	
	#Prepare_Data()
	Predict_Occupancy(user,3,6,5,3) 
	