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

def Extract_Events_GHC(user):
	
	event_fields = ['user_id','event_id','location','time','activity','dow','dom','doy','season','week_weekend']
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','event_id']
	
	data_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "MicroclimateControl_Data")	
	event_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	data_dbHandler.update("UPDATE LeaveTime_Feature_Matrix_%i_final SET current_DOW = WEEKDAY(date)" % (user))
	
	data_result = data_dbHandler.select("SELECT *,DAYOFMONTH(date),DAYOFYEAR(date),MONTH(date) FROM LeaveTime_Feature_Matrix_%i_final WHERE remaining_duration = 0 AND threshold = 1 ORDER BY timestamp ASC" % (user))
	
	last_time_index = -1
	last_duration = -1
	event_id = 1
	
	for row in data_result:
		
		if last_time_index == -1: #Discard first row because no prior data available				
			last_time_index = int(row[9])
			last_duration = int(row[12])
			last_date = row[3]
		else:
			
			values = []
			values.append(user)
			values.append(int(row[2]))
			values.append(row[3])
			values.append(Get_Time(int(row[9])))
			values.append(int(row[9]))
			values.append(int(row[5]))
			values.append(int(row[10]))
			values.append(int(row[11]))
			values.append(int(row[12]))
			values.append((last_time_index+1)%288)
			values.append(last_duration)				
			
			event_string = ""
			
			# Transition-To Event
			event_values = []
			event_values.append(user)
			event_values.append(event_id)
			event_string = "%s%i" % (event_string,event_id)
			event_id = event_id + 1
			event_values.append(int(row[11]))
			event_values.append((last_time_index+1)%288)
			event_values.append("Transition-To")
			event_values.append(int(row[10]))
			event_values.append(int(row[15]))
			event_values.append(int(row[16]))
			
			if int(row[17]) < 4:
				event_values.append(0)
				
			if int(row[17]) >= 4 and int(row[17]) < 7:
				event_values.append(1)
				
			if int(row[17]) >= 7 and int(row[17]) < 10:
				event_values.append(2)
				
			if int(row[17]) >= 10:
				event_values.append(3)
			
			event_values.append(int(row[5]))
			
			event_dbHandler.insert("Events_GHC",event_fields,event_values)
			#print event_values
			
			# Transition-From Event
			event_values = []
			event_values.append(user)
			event_values.append(event_id)
			event_string = "%s,%i" % (event_string,event_id)
			event_id = event_id + 1
			event_values.append(int(row[11]))
			event_values.append(int(row[9]))
			event_values.append("Transition-From")
			event_values.append(int(row[10]))
			event_values.append(int(row[15]))
			event_values.append(int(row[16]))
			
			if int(row[17]) < 4:
				event_values.append(0)
				
			if int(row[17]) >= 4 and int(row[17]) < 7:
				event_values.append(1)
				
			if int(row[17]) >= 7 and int(row[17]) < 10:
				event_values.append(2)
				
			if int(row[17]) >= 10:
				event_values.append(3)
			
			event_values.append(int(row[5]))
			
			event_dbHandler.insert("Events_GHC",event_fields,event_values)
			#print event_values
			
			# Duration Event
			event_values = []
			event_values.append(user)
			event_values.append(event_id)
			event_string = "%s,%i" % (event_string,event_id)
			event_id = event_id + 1
			event_values.append(int(row[11]))
			event_values.append(int(row[12]))
			event_values.append("Duration")
			event_values.append(int(row[10]))
			event_values.append(int(row[15]))
			event_values.append(int(row[16]))
			
			if int(row[17]) < 4:
				event_values.append(0)
				
			if int(row[17]) >= 4 and int(row[17]) < 7:
				event_values.append(1)
				
			if int(row[17]) >= 7 and int(row[17]) < 10:
				event_values.append(2)
				
			if int(row[17]) >= 10:
				event_values.append(3)
			
			event_values.append(int(row[5]))
			
			event_dbHandler.insert("Events_GHC",event_fields,event_values)
			#print event_values				
			
			if int(row[11]) == 0 and row[3] != last_date:
				
				last_date = row[3]
				
				event_values = []
				event_values.append(user)
				event_values.append(event_id)
				event_string = "%s,%i" % (event_string,event_id)
				event_id = event_id + 1
				event_values.append(1)
				event_values.append((int(row[9])+1)%288)
				event_values.append("First Arrival")
				event_values.append(int(row[10]))
				event_values.append(int(row[15]))
				event_values.append(int(row[16]))
				
				if int(row[17]) < 4:
					event_values.append(0)
					
				if int(row[17]) >= 4 and int(row[17]) < 7:
					event_values.append(1)
					
				if int(row[17]) >= 7 and int(row[17]) < 10:
					event_values.append(2)
					
				if int(row[17]) >= 10:
					event_values.append(3)
				
				event_values.append(int(row[5]))
				
				event_dbHandler.insert("Events_GHC",event_fields,event_values)
				#print event_values
				
				event_values = []
				event_values.append(user)
				event_values.append(event_id)
				event_string = "%s,%i" % (event_string,event_id)
				event_id = event_id + 1
				event_values.append(1)
				event_values.append((last_time_index+1)%288)
				event_values.append("Last Departure")
				event_values.append(int(row[10]))
				event_values.append(int(row[15]))
				event_values.append(int(row[16]))
				
				if int(row[17]) < 4:
					event_values.append(0)
					
				if int(row[17]) >= 4 and int(row[17]) < 7:
					event_values.append(1)
					
				if int(row[17]) >= 7 and int(row[17]) < 10:
					event_values.append(2)
					
				if int(row[17]) >= 10:
					event_values.append(3)
				
				event_values.append(int(row[5]))
				
				event_dbHandler.insert("Events_GHC",event_fields,event_values)
				#print event_values

			values.append(event_string)
			event_dbHandler.insert("Data_GHC",data_fields,values)
			#print values
			
			last_time_index = int(row[9])
			last_duration = int(row[12])				
			
def Get_Time(time_index):
	
	hour = int(math.floor((time_index*5)/60))
	minute = time_index*5 - hour*60
	
	time_string = ""
	
	if hour < 10:		
		time_string = "%s0%i:" % (time_string,hour)
	else:
		time_string = "%s%i:" % (time_string,hour)
		
	if minute < 10:
		time_string = "%s0%i:00" % (time_string,minute)
	else:
		time_string = "%s%i:00" % (time_string,minute)
		
	return time_string

def Extract_Temporal_Cluster_GHC(user): # The number of minutes a new event can differ from mean of previous events (5-minute increments)
	
	try:
		event_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
		
		activities = ['Transition-To','Transition-From','Duration','First Arrival','Last Departure']
		#activities = ['Last Departure']
		
		for activity in activities:
			
			for location in range(0,2):					
			
				if not(location == 0 and (activity == 'Last Departure' or activity == 'First Arrival')):
			
					avg_cluster_size = []
					avg_between_cluster_distance = []
					avg_cluster_density = []
					
					std_cluster_size = []
					std_between_cluster_distance = []
					std_cluster_density = []
					
					num_clusters = []
					num_short_clusters = []
					
					dunn_indices = []
					davies_bouldin_indices = []
					
					cluster_result = []
					cluster_result_means = []
					
					for outer_threshold in range(1,8):
						for inner_threshold in range(1,8):
							
							#print activity,location,outer_threshold,inner_threshold
							
							result = event_dbHandler.select("SELECT event_id,time,dow,dom,season,week_weekend FROM Events_GHC WHERE activity = '%s' AND location = %i AND user_id = %i ORDER BY time" % (activity,location,user))
							
							clusters = []
							cluster_means = []
							
							event_ids = []
							event_data = []
							
							num_weekdays = 0
							num_weekends = 0
							weekdays = []
							
							for row in result:
								
								event_ids.append(int(row[0]))
									
								data = []
								for i in range(1,6):
									data.append(int(row[i]))
								event_data.append(data)
								
								weekdays.append(int(row[2]))
								
								if int(row[5]) == 5:
									num_weekdays = num_weekdays + 1
								else:
									num_weekends = num_weekends + 1
								
								if len(clusters) == 0:
									new_cluster = []
									new_cluster.append(int(row[0]))
									clusters.append(new_cluster)
									
									new_mean = []
									new_mean.append(int(row[1]))
									cluster_means.append(new_mean)				
								else:
									tmp_means = []
									
									for mean in cluster_means:
										tmp_means.append(numpy.mean(numpy.array(mean)))
													
									closest_index = Find_Nearest(numpy.array(tmp_means),int(row[1]))
									
									if int(row[1]) < 120 or int(row[1]) > 192:
										threshold = outer_threshold
									else:
										threshold = inner_threshold
									
									if math.fabs(int(row[1])-tmp_means[closest_index]) <= threshold:
										new_cluster = clusters[closest_index]
										new_cluster.append(int(row[0]))					
										clusters[closest_index] = new_cluster
										
										new_mean = cluster_means[closest_index]
										new_mean.append(int(row[1]))
										cluster_means[closest_index] = new_mean
									else:
										new_cluster = []
										new_cluster.append(int(row[0]))
										clusters.append(new_cluster)
										
										new_mean = []
										new_mean.append(int(row[1]))
										cluster_means.append(new_mean)
									
							
							cluster_length = []
							short_clusters = 0
							cluster_densities = []
							
							for i in range(len(clusters)):
								cluster_length.append(len(clusters[i]))
								
								if len(clusters[i]) < 5:
									short_clusters = short_clusters + 1
								
								data = []
								days = []
								
								cluster_densities.append(Calculate_Cluster_Density(cluster_means[i]))
								
								for event in clusters[i]:
									closest_index = Find_Nearest(numpy.array(event_ids),event)
									
									tmp_data = event_data[closest_index]
									data_string = "%i,%i,%i" % (tmp_data[0],tmp_data[1],tmp_data[4])
									data.append(data_string)
									days.append(tmp_data[1])					
							
							previous_mean = 0
							between_cluster_distances = []
							
							for values in cluster_means:
								
								current_mean = numpy.mean(numpy.array(values))
								
								if previous_mean == 0:
									previous_mean = current_mean
								else:
									between_cluster_distances.append(current_mean-previous_mean)
									previous_mean = current_mean
							
							#print between_cluster_distances
							avg_between_cluster_distance.append(numpy.mean(numpy.array(between_cluster_distances)))
							std_between_cluster_distance.append(numpy.std(numpy.array(between_cluster_distances)))
							
							num_clusters.append(len(clusters))
							num_short_clusters.append(short_clusters)
							#print len(event_ids),numpy.mean(numpy.array(cluster_length)),numpy.std(numpy.array(cluster_length))
							avg_cluster_size.append(numpy.mean(numpy.array(cluster_length)))
							std_cluster_size.append(numpy.std(numpy.array(cluster_length)))
							
							avg_cluster_density.append(numpy.mean(numpy.array(cluster_densities)))
							std_cluster_density.append(numpy.std(numpy.array(cluster_densities)))
							
							max_density = cluster_densities[numpy.argmax(numpy.array(cluster_densities))]
							
							dunn_candidates = []				
							
							for i in range(len(cluster_means)):
								for j in range(i+1,len(cluster_means)):
									dunn_candidates.append(float(math.fabs(numpy.mean(numpy.array(cluster_means[i]))-numpy.mean(numpy.array(cluster_means[j]))))/float(max_density))
			
							dunn_indices.append(dunn_candidates[numpy.argmin(numpy.array(dunn_candidates))])
							
							db = 0
							
							for i in range(len(cluster_means)):
								db_tmp = []
								
								for j in range(len(cluster_means)):
									if i != j:
										db_tmp.append(float(cluster_densities[i]+cluster_densities[j])/float(math.fabs(numpy.mean(numpy.array(cluster_means[i]))-numpy.mean(numpy.array(cluster_means[j])))))
									
								db = db + db_tmp[numpy.argmax(numpy.array(db_tmp))]
								
							davies_bouldin_indices.append(float(db)/float(len(clusters)))
							
							cluster_result.append(clusters)
							
							mean_tmp = []
							
							for i in range(len(cluster_means)):
								mean_tmp.append(numpy.mean(numpy.array(cluster_means[i])))
							
							cluster_result_means.append(mean_tmp)
					
					output = ""
					
					fields = ['user_id','activity','outer_temporal_threshold','inner_temporal_threshold','mean_cluster_distance','mean_cluster_diameter','mean_cluster_size','std_cluster_distance','std_cluster_diameter','std_cluster_size','number_clusters','number_short_clusters','dunn_index','davies_bouldin_index']
					
					event_dbHandler.deleteData("DELETE FROM Pattern_Analysis WHERE user_id = %i" % (user))
					
					for outer_threshold in range(1,8):
						for inner_threshold in range(1,8):				
							values = []
							values.append(user)
							values.append(activity)		
							values.append(outer_threshold*5)
							values.append(inner_threshold*5)
							values.append(avg_between_cluster_distance[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(avg_cluster_density[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(avg_cluster_size[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(std_between_cluster_distance[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(std_cluster_density[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(std_cluster_size[(outer_threshold-1)*7+(inner_threshold-1)])		
							values.append(num_clusters[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(num_short_clusters[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(dunn_indices[(outer_threshold-1)*7+(inner_threshold-1)])
							values.append(davies_bouldin_indices[(outer_threshold-1)*7+(inner_threshold-1)])
							
							event_dbHandler.insert("Pattern_Analysis",fields,values)
							
					outer_threshold,inner_threshold = Find_Best_Clustering(user,activity)
					
					best_cluster = cluster_result[(outer_threshold-1)*7+(inner_threshold-1)]
					best_cluster_means = cluster_result_means[(outer_threshold-1)*7+(inner_threshold-1)]										
					
					fields = ['user_id','event_id','activity','location','time','dow','dom','doy','season','week_weekend','temporal_cluster_id','temporal_cluster_centroid']
					
					event_result = event_dbHandler.select("SELECT * FROM Events_GHC WHERE user_id = %i AND activity = '%s'" % (user,activity))
					
					for event in event_result:				
						for i in range(len(best_cluster)):				
							try:
								best_cluster[i].index(int(event[2]))
																		
								current_event = list(event)						
								current_event[11] = Get_Cluster_ID(activity,location,i+1)
								current_event[12] = best_cluster_means[i]
								current_event.pop(0)						
								event_dbHandler.insert("Events_GHC_final",fields,current_event)
							except ValueError, e:
								pass
		return True
	except:
		return False
				
def Extract_Singular_Patterns_GHC(user):
	
	event_dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")

	dates = []
	num_weekdays = []
	num_dom = []
	num_number_in_month = []
	num_week_weekends = []
	num_season = []
	
	result = event_dbHandler.select("SELECT DISTINCT date,WEEKDAY(date),DAYOFMONTH(date),CEIL(DAYOFMONTH(date)/7),week_weekend,MONTH(date) FROM Data_GHC WHERE user_id = %i" % (user))

	for row in result:
		dates.append(row[0])
		num_weekdays.append(int(row[1]))
		num_dom.append(int(row[2]))
		num_number_in_month.append(int(row[3]))
		
		if int(row[4]) == 2:
			num_week_weekends.append(0) # Weekend
		else:
			num_week_weekends.append(1) # Week
			
		if int(row[5]) < 4: 
			num_season.append(0) # Winter
			
		if int(row[5]) >= 4 and int(row[5]) < 7:
			num_season.append(1) # Spring
			
		if int(row[5]) >= 7 and int(row[5]) < 10:
			num_season.append(2) # Summer
			
		if int(row[5]) >= 10:
			num_season.append(3) # Fall
	
	weekdays_hist,tmp = numpy.histogram(num_weekdays,bins=[0,1,2,3,4,5,6,7])
	weekends_hist,tmp = numpy.histogram(num_week_weekends,bins=[0,1,2])
	month_hist,tmp = numpy.histogram(num_dom,bins=range(1,33))
	season_hist,tmp = numpy.histogram(num_season,bins=[0,1,2,3,4])
	
	nim = []	
	
	for i in range(7):
		tmp = numpy.zeros(5,int)
		nim.append(tmp)
	
	for i in range(len(num_number_in_month)):				
		tmp = nim[num_weekdays[i]]		
		tmp[num_number_in_month[i]-1] = tmp[num_number_in_month[i]-1] + 1
		
		nim[num_weekdays[i]] = tmp
	
	result = event_dbHandler.select("SELECT DISTINCT temporal_cluster_id, temporal_cluster_centroid FROM Events_GHC_final WHERE user_id = %i" % (user))
	
	event_clusters = []
	event_cluster_centroids = []
	
	for row in result:
		event_clusters.append(row[0])
		event_cluster_centroids.append(int(row[1]))
		
	pattern_id = 1
	
	fields = ['user_id','pattern_id','activity','location','time','members','probability','dow','dom','doy','season','week_weekend','number_in_month']
	
	for i in range(len(event_clusters)):
		
		event_ids = []
		activity = ""
		location = -1
		dow = []
		dom = []
		doy = []
		season = []
		week_weekend = []
		number_in_month = []
		
		count = 0
		last_doy = 0
		
		result = event_dbHandler.select("SELECT * FROM Events_GHC_final WHERE user_id = %i AND temporal_cluster_id = '%s' ORDER BY doy" % (user,event_clusters[i]))
		
		for row in result:			
			event_ids.append(int(row[2]))
			activity = row[3]
			location = int(row[4])
			dow.append(int(row[6]))
			dom.append(int(row[7]))
			doy.append(int(row[8]))
			season.append(int(row[9]))						
			
			if int(row[10]) == 2:
				week_weekend.append(0) # Weekend
			else:
				week_weekend.append(1) # Week
			
			number_in_month.append(math.ceil(float(row[7])/float(7)))
			
			if last_doy != int(row[8]):
				count = count + 1
			
			last_doy = int(row[8])
					
		
		# Singular Pattern (SP) without contextual information		
		values = []
		values.append(user)
		values.append(pattern_id)
		values.append(activity)
		values.append(location)
		values.append(event_cluster_centroids[i])
		values.append(Get_Member_List(event_ids,range(len(event_ids))))
		
		if len(dates) > 0:
			values.append(float(count)/float(len(dates)))
		else:
			values.append(0)
		
		pattern_id = pattern_id + 1
		
		event_dbHandler.insert("Singular_Pattern_Base",fields[0:7],values)
		
		# SP for Day of Week
		hist,tmp = numpy.histogram(dow,bins=[0,1,2,3,4,5,6,7])
		
		for weekday in range(7):
			if hist[weekday] != 0:
				reps = Find_Elements(dow,weekday)
				
				values = []
				values.append(user)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(weekdays_hist[weekday]) > 0:
					values.append(float(Probability_Count(doy,dow,weekday))/float(weekdays_hist[weekday])) # Probability
				else:					
					values.append(0)
					
				values.append(weekday)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[7])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Day of Month
		hist,tmp = numpy.histogram(dom,bins=range(1,33))
		
		for dayOfMonth in range(1,32):									
			reps = Find_Elements(dom,dayOfMonth)
			
			if len(reps) > 0:
			
				values = []
				values.append(user)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(month_hist[dayOfMonth-1]) > 0:
					values.append(float(Probability_Count(doy,dom,dayOfMonth))/float(month_hist[dayOfMonth-1])) # Probability
				else:
					values.append(0)
					
				values.append(dayOfMonth)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[8])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Season
		hist,tmp = numpy.histogram(season,bins=[0,1,2,3,4])
		
		for current_season in range(4):
			if hist[current_season] != 0:
				reps = Find_Elements(season,current_season)
			
				values = []
				values.append(user)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(season_hist[current_season]) > 0:
					values.append(float(Probability_Count(doy,season,current_season))/float(season_hist[current_season])) # Probability
				else:
					values.append(0)
					
				values.append(current_season)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[10])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Week vs. Weekend
		hist,tmp = numpy.histogram(week_weekend,bins=[0,1,2])
		
		for current_week_weekend in range(2):
			if hist[current_week_weekend] != 0:
				reps = Find_Elements(week_weekend,current_week_weekend)
			
				values = []
				values.append(user)
				values.append(pattern_id)
				values.append(activity)
				values.append(location)
				values.append(event_cluster_centroids[i])
				values.append(Get_Member_List(event_ids,reps))
				
				if float(weekends_hist[current_week_weekend]) > 0:
					values.append(float(Probability_Count(doy,week_weekend,current_week_weekend))/float(weekends_hist[current_week_weekend])) # Probability
				else:
					values.append(0)
					
				values.append(current_week_weekend)
				
				pattern_id = pattern_id + 1
				
				new_fields = fields[0:7]
				new_fields.append(fields[11])				
				
				event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)
				
		# SP for Number in Month
		cluster_nim = []
	
		for l in range(7):			
			tmp = numpy.zeros(5,int)
			cluster_nim.append(tmp)
		
		last_doy = -1
		
		for l in range(len(number_in_month)):							
			if last_doy != doy[l]:			
				tmp = cluster_nim[dow[l]]		
				tmp[number_in_month[l]-1] = tmp[number_in_month[l]-1] + 1
				
				cluster_nim[dow[l]] = tmp
				
			last_doy = doy[l]
		
		for weekday in range(7):
			for number in range(1,6):
				if cluster_nim[weekday][number-1] != 0:
					
					reps = Find_Elements(dow,weekday)
					
					member_list = ""
					
					for l in range(len(reps)-1):
						if number_in_month[reps[l]] == number:
							member_list = "%s%i," % (member_list,event_ids[reps[l]])
		
					if number_in_month[reps[-1]] == number:
						member_list = "%s%i" % (member_list,event_ids[reps[-1]])
			
					values = []
					values.append(user)
					values.append(pattern_id)
					values.append(activity)
					values.append(location)
					values.append(event_cluster_centroids[i])
					values.append(member_list)
					
					if float(nim[weekday][number-1]) > 0:
						values.append(float(cluster_nim[weekday][number-1])/float(nim[weekday][number-1])) # Probability
					else:
						values.append(0)
						
					values.append(weekday)
					values.append(number)
					
					pattern_id = pattern_id + 1
					
					new_fields = fields[0:7]
					new_fields.append(fields[7])				
					new_fields.append(fields[12])				
					
					event_dbHandler.insert("Singular_Pattern_Base",new_fields,values)		

def Map_Singular_Patterns_GHC():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	data_fields = ['id','user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','event_id','singular_pattern_id']
	
	user_ids = []
	
	result = dbHandler.select("SELECT DISTINCT user_id FROM Singular_Pattern_Base WHERE user_id > 2")
	
	for row in result:
		user_ids.append(int(row[0]))
	
	for user in user_ids:
		start = time.time()
		
		dbHandler.truncateTable("Singular_Pattern_Base_tmp")		
		dbHandler.select("INSERT INTO Singular_Pattern_Base_tmp SELECT * FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
		
		result = dbHandler.select("SELECT * FROM Data_GHC WHERE user_id = %i ORDER BY timestamp" % (user))
		
		for row in result:
			
			event_ids = row[12].split(",")
			
			sp_ids = []
			
			for event in event_ids:
				
				select_string = "SELECT pattern_id FROM Singular_Pattern_Base_tmp WHERE user_id =  %i AND INSTR(members,',%i,') > 0 " % (user,int(event))
				
				sp_result = dbHandler.select(select_string)
				
				for sp_row in sp_result:
					sp_ids.append(int(sp_row[0]))
					
			sp_output = ","
			
			for i in range(len(sp_ids)):
				sp_output = "%s%i," % (sp_output,sp_ids[i])
			
			values = []
			values.append(int(row[0]))#id
			values.append(int(row[1]))#user_id
			values.append(int(row[2]))#timestamp
			values.append(row[3])#date
			values.append(row[4])#time
			values.append(int(row[5]))#time_index
			values.append(int(row[6]))#week_weekend
			values.append(int(row[7]))#current_dow
			values.append(int(row[8]))#current_location
			values.append(int(row[9]))#current_duration
			values.append(int(row[10]))#transition_time
			values.append(int(row[11]))#last_duration
			values.append(row[12])#event_id
			values.append(sp_output)#singular_pattern_id
			
			dbHandler.insert("Data_GHC_final",data_fields,values)
			
		print time.time()-start

def Extract_Long_Patterns_GHC(max_interval_length,user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	fields = ["user_id","pattern_id","pattern_members","pattern_length","temporal_interval","probability","most_specific_temporal_condition","interest","cosine","shapiro","jaccard"]
			
	
	temporal_interval_description = []
	temporal_interval_specificity = []
	
	tmp = []
	tmp_specificity = []
	
	for index in range(2+(max_interval_length-1)*2):
		tmp.append("")
		tmp_specificity.append(0)
		
	temporal_interval_description.append(tmp)
	temporal_interval_specificity.append(tmp_specificity)
	
	tmp = []
	
	for index in range(2+(max_interval_length-1)*2):
		tmp.append("")
		tmp_specificity.append(0)
		
	temporal_interval_description.append(tmp)
	temporal_interval_specificity.append(tmp_specificity)
	
	tmp = []
	
	for index in range(2+(max_interval_length-1)*2):
		tmp.append("")
		tmp_specificity.append(0)
		
	temporal_interval_description.append(tmp)
	temporal_interval_specificity.append(tmp_specificity)
		
	
	for index in range(2+(max_interval_length-1)*2):
		if index == 0:
			temporal_interval_description[0][index] = "same_day"
			temporal_interval_description[1][index] = "same_week"
			temporal_interval_description[2][index] = "same_month"
			temporal_interval_specificity[0][index] = 1
			temporal_interval_specificity[1][index] = 0.6667
			temporal_interval_specificity[2][index] = 0.3333
	
		if index == 1:
			temporal_interval_description[0][index] = "adjacent_1_day"
			temporal_interval_description[1][index] = "adjacent_1_week"
			temporal_interval_description[2][index] = "adjacent_1_month"
			temporal_interval_specificity[0][index] = 0.9444
			temporal_interval_specificity[1][index] = 0.6111
			temporal_interval_specificity[2][index] = 0.2778
		
		if index > 1 and index % 2 == 0:
			temporal_interval_description[0][index] = "at_adjacent_%i_day" % (index/2 + 1)
			temporal_interval_description[1][index] = "at_adjacent_%i_week" % (index/2 + 1)
			temporal_interval_description[2][index] = "at_adjacent_%i_month" % (index/2 + 1)
			temporal_interval_specificity[0][index] = 0.8889
			temporal_interval_specificity[1][index] = 0.5556
			temporal_interval_specificity[2][index] = 0.2222
		
		if index > 1 and index % 2 != 0:
			temporal_interval_description[0][index] = "within_adjacent_%i_day" % ((index-1)/2 + 1)
			temporal_interval_description[1][index] = "within_adjacent_%i_week" % ((index-1)/2 + 1)
			temporal_interval_description[2][index] = "within_adjacent_%i_month" % ((index-1)/2 + 1)
			temporal_interval_specificity[0][index] = 0.7778
			temporal_interval_specificity[1][index] = 0.4444
			temporal_interval_specificity[2][index] = 0.1111
	
	dbHandler.truncateTable("Data_GHC_tmp")		
	dbHandler.select("INSERT INTO Data_GHC_tmp SELECT * FROM Data_GHC_final WHERE user_id = %i" % (user))
	dbHandler.update("UPDATE Data_GHC_tmp SET event_id = CONCAT(',',event_id,',')")
	
	singular_patterns = []
	
	result = dbHandler.select("SELECT probability FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i" % (user))
	
	probabilities_sp = []
	
	for row in result:
		probabilities_sp.append(float(row[0]))
		
	prob_median = numpy.median(probabilities_sp)
	
	result = dbHandler.select("SELECT * FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i AND probability > %f" % (user,prob_median))
	
	current_sp_collection = []
	
	last_activity = ""
	last_location = -1
	last_time = -1
	
	for row in result:
		
		if last_activity == "":
			last_activity = row[3]
			last_location = int(row[4])
			last_time = int(row[5])
			
		if last_activity == row[3] and last_location == int(row[4]) and last_time == int(row[5]):
			current_sp_collection.append(row)
		else:
			singular_patterns.append(current_sp_collection)
			
			current_sp_collection = []
			current_sp_collection.append(row)
			
		last_activity = row[3]
		last_location = int(row[4])
		last_time = int(row[5])
		
	pattern_id = 0
	
	result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		pattern_id = int(row[0]) + 1
	
	previous_patterns = []
	
	result = dbHandler.select("SELECT * FROM Data_GHC_tmp ORDER BY id DESC LIMIT 1")
	
	max_event_id = -1
	
	for row in result:
		members = row[12].split(",")
		
		max_event_id = int(members[-2])
		
	event_array = []
	
	for k in range(max_event_id):
		event_array.append(None)
	
	result = dbHandler.select("SELECT * FROM Data_GHC_tmp")
	
	for row in result:
		
		members = row[12].split(",")
		date_tmp = datetime.datetime.fromtimestamp(int(row[2]))
		date_tmp = date_tmp.replace(hour=0,minute=0,second=0)
		
		for member in range(1,len(members)-1):
			event_array[int(members[member])-1] = date_tmp
		
	for pattern_length in range(2,3):
		start = time.time()
		
		if pattern_length == 2:
			patterns = []
			
			for i in range(len(singular_patterns)):
				for k in range(len(singular_patterns[i])):
					for j in xrange(len(singular_patterns)):
						if i != j:
							for l in range(len(singular_patterns[j])):
								if singular_patterns[j][l][3] != 'Duration':
									new_pattern = []
									new_pattern.append(singular_patterns[i][k])
									new_pattern.append(singular_patterns[j][l])
									patterns.append(new_pattern)
		else:
			pass
		
		previous_patterns = []
		
		# Calculate Unconditional Probability of P
		
		# Total counts
		week_weekend = numpy.zeros(2,int)
		season = numpy.zeros(4,int)
		day_of_week = numpy.zeros(7,int)
		day_of_month = numpy.zeros(31,int)
		number_in_month = numpy.zeros((7,5),int)
		daily = 0
		
		insert_patterns = []
		
		for i in range(len(patterns)):#len(patterns)
			
			if len(insert_patterns) >= 10000:				
				dbHandler.insert_bulk("Pattern_Base",fields,insert_patterns)
				insert_patterns = []
			
			current_pattern = patterns[i]
			sp_probabilities = []
			
			probability_day_interval = numpy.zeros(2+(max_interval_length-1)*2,float) # Same Day, at Adjacent 1st Day, at Adjacent 2nd Day, within Adjacent 2nd Day
			probability_week_interval = numpy.zeros(2+(max_interval_length-1)*2,float)
			probability_month_interval = numpy.zeros(2+(max_interval_length-1)*2,float)
			
			choice = -1
			
			# Day of Month
			if any(current_pattern[k][8] != None for k in range(len(current_pattern))) and any(current_pattern[k][13] != None for k in range(len(current_pattern))): # Number in Month
				choice = 0					
			else:
				if any(current_pattern[k][9] != None for k in range(len(current_pattern))):	# Day of Month									
					choice = 1	
				else:
					if any(current_pattern[k][8] != None for k in range(len(current_pattern))): # Day of Week
						choice = 2				
					else:
						if any(current_pattern[k][11] != None for k in range(len(current_pattern))): # Season
							choice = 3
						else:
							if any(current_pattern[k][12] != None for k in range(len(current_pattern))): # Week vs. Weekend
								choice = 4
							else: # Daily
								choce = 5
								
								
			pattern_description = []
			pattern_types = []
			
			for k in range(len(current_pattern)):
				sp_probabilities.append(float(current_pattern[k][7]))
				pattern_types.append(current_pattern[k][3])
				pattern_description.append(",%i," % (int(current_pattern[k][2])))
			
			most_specific_pattern = ""
			
			for k in range(len(current_pattern)):				
				if choice == 0:
					if current_pattern[k][8] != None and current_pattern[k][13] != None:
						most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
						break
				
				if choice == 1:
					if current_pattern[k][9] != None:
						most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
						break
					
				if choice == 2:
					if current_pattern[k][8] != None:
						most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
						break
					
				if choice == 3:
					if current_pattern[k][11] != None:
						most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
						break
					
				if choice == 4:
					if current_pattern[k][12] != None:
						most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
						break
					
				if choice == 5:
					most_specific_pattern = ",%i," % (int(current_pattern[k][2]))
					break
							
			
			events_total = []
			events_total_pattern_id = []
			
			for k in range(len(current_pattern)):
				members = current_pattern[k][6].split(",")
				
				for member in range(1,len(members)-1):
					events_total.append(int(members[member]))
					events_total_pattern_id.append(",%i," % (int(current_pattern[k][2])))					
					
			dates_for_sp = []
			pattern_result = []
			
			unique_pattern_ids = list(set(events_total_pattern_id))
			unique_dates = []
			
			for k in range(len(unique_pattern_ids)):
				unique_dates.append([])									
			
			events_total_new = []
			events_total_pattern_id_new = []
			
			for k in range(len(events_total)):				
				pattern_ind = unique_pattern_ids.index(events_total_pattern_id[k])
				
				if not event_array[events_total[k]-1] in unique_dates[pattern_ind]:
					unique_dates[pattern_ind].append(event_array[events_total[k]-1])
					dates_for_sp.append(event_array[events_total[k]-1])
					events_total_new.append(events_total[k])
					events_total_pattern_id_new.append(events_total_pattern_id[k])							
					
			events_total = events_total_new
			events_total_pattern_id = events_total_pattern_id_new
			
			instance_combinations = []
			most_specific_instances = []
			
			for possible_instance in itertools.combinations(range(len(events_total)),pattern_length):
				valid = True
				
				for o in xrange(len(possible_instance)):
					if not pattern_description[o] in events_total_pattern_id[possible_instance[o]]:
						valid = False
						break
					
					if o > 0:						
						if events_total[possible_instance[o]] < events_total[possible_instance[o-1]]:
							valid = False
							break
				if valid:
					instance_combinations.append(list(possible_instance))
					
					for o in range(len(possible_instance)):
						if pattern_description[o] == most_specific_pattern and not possible_instance[o] in most_specific_instances:
							most_specific_instances.append(possible_instance[o])
								
			
			day_count = numpy.zeros(2+(max_interval_length-1)*2,int) # Same Day, at Adjacent 1st Day, at Adjacent 2nd Day, within Adjacent 2nd Day
			week_count = numpy.zeros(2+(max_interval_length-1)*2,int)
			month_count = numpy.zeros(2+(max_interval_length-1)*2,int)			
			
			used_instances_for_day_count = [] # Same Day, at Adjacent 1st Day, at Adjacent 2nd Day, within Adjacent 2nd Day
			used_instances_for_week_count = []
			used_instances_for_month_count = []
			
			for k in range(2+(max_interval_length-1)*2):
				used_instances_for_day_count.append([])
				used_instances_for_week_count.append([])
				used_instances_for_month_count.append([])
			
			for instance in instance_combinations:
				dates = []
				weeks = []
				min_date = None
				max_date = None
				
				for k in range(len(instance)):					
					dates.append(dates_for_sp[instance[k]])
					
				min_date = min(dates)
				max_date = max(dates)
				
				unique_dates = list(set(dates))						
				unique_weeks = []
				unique_month = []
				
				for m in range(len(dates)):
					unique_weeks.append(dates[m].isocalendar()[1])
					unique_month.append(dates[m].month)
										
				unique_weeks = list(set(unique_weeks))
				unique_month = list(set(unique_month))
				
				# Calculate Day Counts
				if min_date == max_date and not any(instance[k] in used_instances_for_day_count[0] for k in xrange(len(instance))):
					day_count[0] = day_count[0] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_day_count[0].append(instance[k])
							break
					
				if (min_date + datetime.timedelta(days=1)) >= max_date and not any((instance[k]) in used_instances_for_day_count[1] for k in xrange(len(instance))):
					day_count[1] = day_count[1] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_day_count[1].append(instance[k])
							break
				
				for interval_length in range(2,max_interval_length+1):							
					if (min_date + datetime.timedelta(days=interval_length)) == max_date and len(unique_dates) <= 2 and not any((instance[k]) in used_instances_for_day_count[(interval_length-1)*2] for k in xrange(len(instance))):
						day_count[(interval_length-1)*2] = day_count[(interval_length-1)*2] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:
								used_instances_for_day_count[(interval_length-1)*2].append(instance[k])
								break
						
					if (min_date + datetime.timedelta(days=interval_length)) >= max_date and not any((instance[k]) in used_instances_for_day_count[(interval_length-1)*2+1] for k in xrange(len(instance))):
						day_count[(interval_length-1)*2+1] = day_count[(interval_length-1)*2+1] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:
								used_instances_for_day_count[(interval_length-1)*2+1].append(instance[k])
								break							
				
				# Calculate Week Counts
				if min_date.isocalendar()[1] == max_date.isocalendar()[1] and not any((instance[k]) in used_instances_for_week_count[0] for k in xrange(len(instance))):
					week_count[0] = week_count[0] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_week_count[0].append(instance[k])
							break
					
				if (min_date + datetime.timedelta(weeks=1)).isocalendar()[1] >= max_date.isocalendar()[1] and not any((instance[k]) in used_instances_for_week_count[1] for k in xrange(len(instance))):
					week_count[1] = week_count[1] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_week_count[1].append(instance[k])
							break
				
				for interval_length in range(2,max_interval_length+1):
					if (min_date + datetime.timedelta(weeks=interval_length)).isocalendar()[1] == max_date.isocalendar()[1] and len(unique_weeks) <= 2 and not any((instance[k]) in used_instances_for_week_count[(interval_length-1)*2] for k in xrange(len(instance))):
						week_count[(interval_length-1)*2] = week_count[(interval_length-1)*2] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:
								used_instances_for_week_count[(interval_length-1)*2].append(instance[k])
								break
						
					if (min_date + datetime.timedelta(weeks=interval_length)).isocalendar()[1] >= max_date.isocalendar()[1] and not any((instance[k]) in used_instances_for_week_count[(interval_length-1)*2+1] for k in xrange(len(instance))):
						week_count[(interval_length-1)*2+1] = week_count[(interval_length-1)*2+1] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:
								used_instances_for_week_count[(interval_length-1)*2+1].append(instance[k])
								break
				
				# Calculate Month Counts
				if min_date.month == max_date.month and not any((instance[k]) in used_instances_for_month_count[0] for k in xrange(len(instance))):
					month_count[0] = month_count[0] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_month_count[0].append(instance[k])
							break
					
				if (min_date + datetime.timedelta(days=30)).month >= max_date.month and not any((instance[k]) in used_instances_for_month_count[1] for k in xrange(len(instance))):
					month_count[1] = month_count[1] + 1
					
					for k in xrange(len(instance)):
						if (instance[k]) in most_specific_instances:
							used_instances_for_month_count[1].append(instance[k])
							break
				"""
				for interval_length in range(2,max_interval_length+1):							
					if (min_date + datetime.timedelta(days=30*interval_length)).month == max_date.month and len(unique_month) <= 2 and not any((instance[k]) in used_instances_for_month_count[(interval_length-1)*2] for k in xrange(len(instance))):
						month_count[(interval_length-1)*2] = month_count[(interval_length-1)*2] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:
								used_instances_for_month_count[(interval_length-1)*2].append(instance[k])
								break															
					
					if (min_date + datetime.timedelta(days=30*interval_length)).month >= max_date.month and not any((instance[k]) in used_instances_for_month_count[(interval_length-1)*2+1] for k in xrange(len(instance))):
						month_count[(interval_length-1)*2+1] = month_count[(interval_length-1)*2+1] + 1
						
						for k in xrange(len(instance)):
							if (instance[k]) in most_specific_instances:										
								used_instances_for_month_count[(interval_length-1)*2+1].append(instance[k])
								break
				"""
			
			pattern_description_db = ","
			
			for pattern_index in range(len(current_pattern)):
				pattern_description_db = "%s%i," % (pattern_description_db,int(current_pattern[pattern_index][2]))						
			
			if choice == 0: # Number in Month
				for pattern_index in range(len(current_pattern)):
				
					if current_pattern[pattern_index][8] != None and current_pattern[pattern_index][13] != None:
						if number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1] > 0:
							for k in range(len(day_count)):
								probability_day_interval[k] = float(day_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])
								probability_week_interval[k] = float(week_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])								
								probability_month_interval[k] = float(month_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])
						else:
							result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND current_dow = %i AND CEIL(DAYOFMONTH(date)/7) = %i" % (user,int(current_pattern[pattern_index][8]),int(current_pattern[pattern_index][13])))
							
							for row in result:
								number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1] = int(row[0])
								
							if number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1] > 0:
								for k in range(len(day_count)):
									probability_day_interval[k] = float(day_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])
									probability_week_interval[k] = float(week_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])									
									probability_month_interval[k] = float(month_count[k])/float(number_in_month[int(current_pattern[pattern_index][8])-1][int(current_pattern[pattern_index][13])-1])
						break
								
				
				if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
					previous_patterns.append(pattern_description)
											
					for k in range(len(day_count)):
						if probability_day_interval[k] != 0:
							values = []
							values.append(user)
							values.append(pattern_id)
							pattern_id = pattern_id + 1
							values.append(pattern_description_db)
							values.append(pattern_length)
							values.append("%s" % (temporal_interval_description[0][k]))
							values.append(probability_day_interval[k])
							values.append("Number in Month")
							#values.append(float(probability_day_interval[k] + )/float())
							values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
							values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
							values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
							values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
							
							insert_patterns.append(values)
							
						if probability_week_interval[k] != 0:
							values = []
							values.append(user)
							values.append(pattern_id)
							pattern_id = pattern_id + 1
							values.append(pattern_description_db)
							values.append(pattern_length)
							values.append("%s" % (temporal_interval_description[1][k]))
							values.append(probability_week_interval[k])
							values.append("Number in Month")
							values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
							values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
							values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
							values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
							
							insert_patterns.append(values)
							
						if probability_month_interval[k] != 0:
							values = []
							values.append(user)
							values.append(pattern_id)
							pattern_id = pattern_id + 1
							values.append(pattern_description_db)
							values.append(pattern_length)
							values.append("%s" % (temporal_interval_description[2][k]))
							values.append(probability_month_interval[k])
							values.append("Number in Month")
							values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
							values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
							values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
							values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
							
							insert_patterns.append(values)
			else:
				if choice == 1: # Day of Month
					for pattern_index in range(len(current_pattern)):
					
						if current_pattern[pattern_index][9] != None:
							if day_of_month[int(current_pattern[pattern_index][9])-1] > 0:
								for k in range(len(day_count)):
									probability_day_interval[k] = float(day_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
									probability_week_interval[k] = float(week_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
									probability_month_interval[k] = float(month_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
							else:
								result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND DAYOFMONTH(date) = %i" % (user,int(current_pattern[pattern_index][9])))
								
								for row in result:
									day_of_month[int(current_pattern[pattern_index][9])-1] = int(row[0])
									
								if day_of_month[int(current_pattern[pattern_index][9])-1] > 0:
									for k in range(len(day_count)):
										probability_day_interval[k] = float(day_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
										probability_week_interval[k] = float(week_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
										probability_month_interval[k] = float(month_count[k])/float(day_of_month[int(current_pattern[pattern_index][9])-1])
							break
									
					
					if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
						previous_patterns.append(pattern_description)
						
						for k in range(len(day_count)):
							if probability_day_interval[k] != 0:
								values = []
								values.append(user)
								values.append(pattern_id)
								pattern_id = pattern_id + 1
								values.append(pattern_description_db)
								values.append(pattern_length)
								values.append("%s" % (temporal_interval_description[0][k]))
								values.append(probability_day_interval[k])
								values.append("Day of Month")
								values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
								values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
								values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
								values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
								
								insert_patterns.append(values)
								
							if probability_week_interval[k] != 0:
								values = []
								values.append(user)
								values.append(pattern_id)
								pattern_id = pattern_id + 1
								values.append(pattern_description_db)
								values.append(pattern_length)
								values.append("%s" % (temporal_interval_description[1][k]))
								values.append(probability_week_interval[k])
								values.append("Day of Month")
								values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
								values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
								values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
								values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
								
								insert_patterns.append(values)
								
							if probability_month_interval[k] != 0:
								values = []
								values.append(user)
								values.append(pattern_id)
								pattern_id = pattern_id + 1
								values.append(pattern_description_db)
								values.append(pattern_length)
								values.append("%s" % (temporal_interval_description[2][k]))
								values.append(probability_month_interval[k])
								values.append("Day of Month")
								values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
								values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
								values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
								values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
								
								insert_patterns.append(values)
				else:		
					if choice == 2: # Day of Week
						for pattern_index in range(len(current_pattern)):
						
							if current_pattern[pattern_index][8] != None:							
								if day_of_week[int(current_pattern[pattern_index][8])-1] > 0:
									for k in range(len(day_count)):
										probability_day_interval[k] = float(day_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
										probability_week_interval[k] = float(week_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
										probability_month_interval[k] = float(month_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
								else:
									result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND current_dow = %i" % (user,int(current_pattern[pattern_index][8])))
									
									for row in result:
										day_of_week[int(current_pattern[pattern_index][8])-1] = int(row[0])
										
									if day_of_week[int(current_pattern[pattern_index][8])-1] > 0:
										for k in range(len(day_count)):
											probability_day_interval[k] = float(day_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
											probability_week_interval[k] = float(week_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
											probability_month_interval[k] = float(month_count[k])/float(day_of_week[int(current_pattern[pattern_index][8])-1])
								break
										
						
						if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
							previous_patterns.append(pattern_description)
							
							for k in range(len(day_count)):
								if probability_day_interval[k] != 0:
									values = []
									values.append(user)
									values.append(pattern_id)
									pattern_id = pattern_id + 1
									values.append(pattern_description_db)
									values.append(pattern_length)
									values.append("%s" % (temporal_interval_description[0][k]))
									values.append(probability_day_interval[k])
									values.append("Day of Week")
									values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
									values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
									values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
									values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
									
									insert_patterns.append(values)
									
								if probability_week_interval[k] != 0:
									values = []
									values.append(user)
									values.append(pattern_id)
									pattern_id = pattern_id + 1
									values.append(pattern_description_db)
									values.append(pattern_length)
									values.append("%s" % (temporal_interval_description[1][k]))
									values.append(probability_week_interval[k])
									values.append("Day of Week")
									values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
									values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
									values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
									values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
									
									insert_patterns.append(values)
									
								if probability_month_interval[k] != 0:
									values = []
									values.append(user)
									values.append(pattern_id)
									pattern_id = pattern_id + 1
									values.append(pattern_description_db)
									values.append(pattern_length)
									values.append("%s" % (temporal_interval_description[2][k]))
									values.append(probability_month_interval[k])
									values.append("Day of Week")
									values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
									values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
									values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
									values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
									
									insert_patterns.append(values)
					else:		
						if choice == 3: # Season
							for pattern_index in range(len(current_pattern)):
							
								if current_pattern[pattern_index][11] != None:							
									if season[int(current_pattern[pattern_index][11])-1] > 0:
										for k in range(len(day_count)):
											probability_day_interval[k] = float(day_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
											probability_week_interval[k] = float(week_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
											probability_month_interval[k] = float(month_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
									else:
										if int(current_pattern[pattern_index][11]) == 0:
											result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND MONTH(date) < 4" % (user))
											
										if int(current_pattern[pattern_index][11]) == 1:
											result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND MONTH(date) >= 4 AND MONTH(date) < 7" % (user))
											
										if int(current_pattern[pattern_index][11]) == 2:
											result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND MONTH(date) >= 7 AND MONTH(date) < 10" % (user))
											
										if int(current_pattern[pattern_index][11]) == 3:
											result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND MONTH(date) >= 10" % (user))
										
										for row in result:
											season[int(current_pattern[pattern_index][11])-1] = int(row[0])
											
										if season[int(current_pattern[pattern_index][11])-1] > 0:
											for k in range(len(day_count)):
												probability_day_interval[k] = float(day_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
												probability_week_interval[k] = float(week_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
												probability_month_interval[k] = float(month_count[k])/float(season[int(current_pattern[pattern_index][11])-1])
									break
											
							
							if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
								previous_patterns.append(pattern_description)
								
								for k in range(len(day_count)):
									if probability_day_interval[k] != 0:
										values = []
										values.append(user)
										values.append(pattern_id)
										pattern_id = pattern_id + 1
										values.append(pattern_description_db)
										values.append(pattern_length)
										values.append("%s" % (temporal_interval_description[0][k]))
										values.append(probability_day_interval[k])
										values.append("Season")
										values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
										values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
										values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
										values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
										
										insert_patterns.append(values)
										
									if probability_week_interval[k] != 0:
										values = []
										values.append(user)
										values.append(pattern_id)
										pattern_id = pattern_id + 1
										values.append(pattern_description_db)
										values.append(pattern_length)
										values.append("%s" % (temporal_interval_description[1][k]))
										values.append(probability_week_interval[k])
										values.append("Season")
										values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
										values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
										values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
										values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
										
										insert_patterns.append(values)
										
									if probability_month_interval[k] != 0:
										values = []
										values.append(user)
										values.append(pattern_id)
										pattern_id = pattern_id + 1
										values.append(pattern_description_db)
										values.append(pattern_length)
										values.append("%s" % (temporal_interval_description[2][k]))
										values.append(probability_month_interval[k])
										values.append("Season")
										values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
										values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
										values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
										values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
										
										insert_patterns.append(values)
						else:		
							if choice == 4: # Week vs. Weekend
								for pattern_index in range(len(current_pattern)):
								
									if current_pattern[pattern_index][12] != None:							
										if week_weekend[int(current_pattern[pattern_index][12])-1] > 0:
											for k in range(len(day_count)):
												probability_day_interval[k] = float(day_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
												probability_week_interval[k] = float(week_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
												probability_month_interval[k] = float(month_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
										else:
											if int(current_pattern[pattern_index][12]) == 0:
												result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND week_weekend = 2" % (user))
											
											if int(current_pattern[pattern_index][12]) == 1:
												result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i AND week_weekend = 5" % (user))
											
											for row in result:
												week_weekend[int(current_pattern[pattern_index][12])-1] = int(row[0])
												
											if week_weekend[int(current_pattern[pattern_index][12])-1] > 0:
												for k in range(len(day_count)):
													probability_day_interval[k] = float(day_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
													probability_week_interval[k] = float(week_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
													probability_month_interval[k] = float(month_count[k])/float(week_weekend[int(current_pattern[pattern_index][12])-1])
										break
																				
								if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
									previous_patterns.append(pattern_description)
									
									for k in range(len(day_count)):
										if probability_day_interval[k] != 0:
											values = []
											values.append(user)
											values.append(pattern_id)
											pattern_id = pattern_id + 1
											values.append(pattern_description_db)
											values.append(pattern_length)
											values.append("%s" % (temporal_interval_description[0][k]))
											values.append(probability_day_interval[k])
											values.append("Week vs. Weekend")
											values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
											values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
											values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
											values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
											
											insert_patterns.append(values)
											
										if probability_week_interval[k] != 0:
											values = []
											values.append(user)
											values.append(pattern_id)
											pattern_id = pattern_id + 1
											values.append(pattern_description_db)
											values.append(pattern_length)
											values.append("%s" % (temporal_interval_description[1][k]))
											values.append(probability_week_interval[k])
											values.append("Week vs. Weekend")
											values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
											values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
											values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
											values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
											
											insert_patterns.append(values)
											
										if probability_month_interval[k] != 0:
											values = []
											values.append(user)
											values.append(pattern_id)
											pattern_id = pattern_id + 1
											values.append(pattern_description_db)
											values.append(pattern_length)
											values.append("%s" % (temporal_interval_description[2][k]))
											values.append(probability_month_interval[k])
											values.append("Week vs. Weekend")
											values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
											values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
											values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
											values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
											
											insert_patterns.append(values)
							else:		
								if choice == 5: # Daily					
									if daily > 0:
										for k in range(len(day_count)):
											probability_day_interval[k] = float(day_count[k])/float(daily)
											probability_week_interval[k] = float(week_count[k])/float(daily)
											probability_month_interval[k] = float(month_count[k])/float(daily)
									else:
										result = dbHandler.select("SELECT count(DISTINCT(date)) FROM Data_GHC_tmp WHERE user_id = %i" % (user))
										
										for row in result:
											daily = int(row[0])
											
										if daily > 0:
											for k in range(len(day_count)):
												probability_day_interval[k] = float(day_count[k])/float(daily)
												probability_week_interval[k] = float(week_count[k])/float(daily)
												probability_month_interval[k] = float(month_count[k])/float(daily)							
													
									
									if not numpy.amax(probability_day_interval) == 0 or not numpy.amax(probability_week_interval) == 0 or not numpy.amax(probability_month_interval) == 0:
										previous_patterns.append(pattern_description)
										
										for k in range(len(day_count)):
											if probability_day_interval[k] != 0:
												values = []
												values.append(user)
												values.append(pattern_id)
												pattern_id = pattern_id + 1
												values.append(pattern_description_db)
												values.append(pattern_length)
												values.append("%s" % (temporal_interval_description[0][k]))
												values.append(probability_day_interval[k])
												values.append("Daily")
												values.append(float(probability_day_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
												values.append(float(probability_day_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
												values.append(float(probability_day_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
												values.append(float(probability_day_interval[k])/float(numpy.sum(sp_probabilities) - probability_day_interval[k])) #Jaccard
												
												insert_patterns.append(values)
												
											if probability_week_interval[k] != 0:
												values = []
												values.append(user)
												values.append(pattern_id)
												pattern_id = pattern_id + 1
												values.append(pattern_description_db)
												values.append(pattern_length)
												values.append("%s" % (temporal_interval_description[1][k]))
												values.append(probability_week_interval[k])
												values.append("Daily")
												values.append(float(probability_week_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
												values.append(float(probability_week_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
												values.append(float(probability_week_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
												values.append(float(probability_week_interval[k])/float(numpy.sum(sp_probabilities) - probability_week_interval[k])) #Jaccard
												
												insert_patterns.append(values)
												
											if probability_month_interval[k] != 0:
												values = []
												values.append(user)
												values.append(pattern_id)
												pattern_id = pattern_id + 1
												values.append(pattern_description_db)
												values.append(pattern_length)
												values.append("%s" % (temporal_interval_description[2][k]))
												values.append(probability_month_interval[k])
												values.append("Daily")
												values.append(float(probability_month_interval[k])/float(numpy.prod(sp_probabilities))) #Interest
												values.append(float(probability_month_interval[k])/float(math.sqrt(numpy.prod(sp_probabilities)))) #Cosine
												values.append(float(probability_month_interval[k]) - float(numpy.prod(sp_probabilities))) #Shapiro
												values.append(float(probability_month_interval[k])/float(numpy.sum(sp_probabilities) - probability_month_interval[k])) #Jaccard
												
												insert_patterns.append(values)
							
		dbHandler.insert_bulk("Pattern_Base",fields,insert_patterns)
	
	print time.time() - start

def Extract_Conditional_Patterns(user,max_interval_length,pool):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	dbHandler_pattern = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Instances")
	
	fields = ["user_id","pattern_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	dbHandler.truncateTable("Data_GHC_tmp")
	dbHandler.update("INSERT INTO Data_GHC_tmp SELECT * FROM Data_GHC_final WHERE user_id = %i" % (user))
	dbHandler.createTable("Pattern_Base_%i" % (user),"SELECT * FROM Pattern_Base_template")
	dbHandler.update("ALTER TABLE Pattern_Base_%i ENGINE = MyISAM" % (user))
	dbHandler_pattern.createTable("Valid_Instances_%i" % (user),"SELECT * FROM Valid_Instances")
	dbHandler_pattern.update("ALTER TABLE Valid_Instances_%i ENGINE = MyISAM" % (user))
	
	temporal_interval_description = []
	temporal_interval_specificity = []
		
	temporal_interval_description.append("same_month")
	temporal_interval_description.append("within_adjacent_2_week")
	temporal_interval_description.append("within_adjacent_1_week")
	temporal_interval_description.append("same_week")
	temporal_interval_description.append("within_adjacent_2_day")
	temporal_interval_description.append("within_adjacent_1_day")
	temporal_interval_description.append("same_day")
	
	for k in range(1,8):
		temporal_interval_specificity.append(0.14285714*k)
				
	singular_pattern_id = []
	singular_pattern_specificity = []
	
	result = dbHandler.select("SELECT DISTINCT pattern_id,specificity FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (user))
	
	for row in result:
		singular_pattern_id.append(int(row[0]))
		singular_pattern_specificity.append(int(row[1]))
	
	timestamps = []
	dates = []
	weeks = []
	months = []
	
	for k in range(max(singular_pattern_id)):
		timestamps.append(-1)
		dates.append("")
		weeks.append(-1)
		months.append(-1)
	
	for sp_pattern_id in singular_pattern_id:
			
		result = dbHandler.select("SELECT timestamp,date,WEEK(date),MONTH(date) FROM Data_GHC_tmp WHERE INSTR(singular_pattern_id,',%i,') > 0 GROUP BY date ORDER BY timestamp" % (sp_pattern_id))
		
		timestamps_tmp = []
		dates_tmp = []
		weeks_tmp = []
		months_tmp = []
		
		for row in result:
			timestamps_tmp.append(int(row[0]))
			dates_tmp.append(row[1])
			weeks_tmp.append(int(row[2]))
			months_tmp.append(int(row[3]))
			
		timestamps[sp_pattern_id-1] = (timestamps_tmp)
		dates[sp_pattern_id-1] = (dates_tmp)
		weeks[sp_pattern_id-1] = (weeks_tmp)
		months[sp_pattern_id-1] = (months_tmp)
	
	singular_patterns = []
	
	result = dbHandler.select("SELECT probability FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 3 AND user_id = %i" % (user))
	
	probabilities_sp = []
	
	for row in result:
		probabilities_sp.append(float(row[0]))
		
	prob_median = numpy.median(probabilities_sp)
	
	result = dbHandler.select("SELECT * FROM Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 3 AND user_id = %i ORDER BY pattern_id" % (user))
	
	current_sp_collection = []
	singular_patterns = []
	singular_patterns_collection = []
	duration_group = []
	non_duration_group = []
	non_duration_group_int = []
	
	last_activity = ""
	last_location = -1
	last_time = -1
	
	for row in result:
		
		singular_patterns.append(row[2])
		
		if row[3] == 'Duration':
			duration_group.append(row[2])
		else:
			non_duration_group.append(row[2])
			non_duration_group_int.append(int(row[2]))
		
		if last_activity == "":
			last_activity = row[3]
			last_location = row[4]
			last_time = row[5]
			
		if last_activity == row[3] and last_location == row[4] and last_time == row[5]:
			current_sp_collection.append(row[2])
		else:
			singular_patterns_collection.append(current_sp_collection)
			current_sp_collection = []
			current_sp_collection.append(row[2])
			
		last_activity = row[3]
		last_location = row[4]
		last_time = row[5]
	
	singular_patterns_collection.append(current_sp_collection)	
	
	current_pattern_id = len(singular_pattern_id) + 1
		
	
	pattern_combinations = []
	pattern_combinations_temporal_intervals = []
	pattern_combinations_temporal_specificity = []
	pattern_combinations_count = []
	
	for pattern_length in range(2,10):						
		
		del pattern_combinations
		del pattern_combinations_temporal_intervals
		del pattern_combinations_temporal_specificity
		del pattern_combinations_count
		
		gc.collect()
		
		pattern_combinations = []
		pattern_combinations_temporal_intervals = []
		pattern_combinations_temporal_specificity = []
		pattern_combinations_count = []
		
		target_index = 0
		count = 0
		
		if pattern_length == 2:
			singular_pattern_count = []
			
			for k in range(len(singular_pattern_id)):
				result = dbHandler.select("SELECT count(id) FROM Data_GHC_tmp WHERE user_id = %i AND INSTR(singular_pattern_id,',%s,') > 0" % (user,singular_pattern_id[k]))
				
				for row in result:
					singular_pattern_count.append(float(row[0]))
			
			combinations = itertools.permutations(range(len(singular_patterns)),2)						
			
			for comb in combinations:
				comb = list(comb)
				
				valid = True
				
				if singular_patterns[comb[-1]] in non_duration_group:
					for sp_collection in singular_patterns_collection:
						if all(singular_patterns[comb[k]] in sp_collection for k in range(len(comb))):
							valid = False
				
				if valid:
					tmp = []
					most_specific = singular_pattern_specificity[singular_patterns[comb[-1]]-1]
					
					for k in range(len(comb)):
						tmp.append(singular_patterns[comb[k]])
										
					pattern_combinations.append(tmp)
					
					new_temporal_interval_description = []
					new_temporal_interval_specificity = []
					
					if most_specific == 0:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
					
					if most_specific == 1:
						for k in range(3,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 2:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 3:
						for k in range(6,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 4:
						for k in range(0,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
						
					if most_specific == 5:
						for k in range(0,7):
							new_temporal_interval_description.append(temporal_interval_description[k])
							new_temporal_interval_specificity.append(temporal_interval_specificity[k])
					
					pattern_combinations_temporal_intervals.append(new_temporal_interval_description)
					pattern_combinations_temporal_specificity.append(new_temporal_interval_specificity)					
					
					sp_count = []
					for k in range(len(new_temporal_interval_description)):
						sp_count.append(singular_pattern_count[tmp[0]-1])
						
					pattern_combinations_count.append(sp_count)
					
		else:
			result = dbHandler.select("SELECT count(DISTINCT pattern_members) FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = %i"  % (user,pattern_length-1))
			
			valid_patterns_count = 0
			
			for row in result:
				valid_patterns_count = int(row[0])
			
			if valid_patterns_count < 100000:
			
				result = dbHandler.select("SELECT DISTINCT pattern_members FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = %i"  % (user,pattern_length-1))
				
				valid_patterns = {}
				valid_patterns_2 = {}
				
				del valid_patterns
				del valid_patterns_2
				
				gc.collect()
				
				valid_patterns = {}
				valid_patterns_2 = {}
				
				valid_patterns_count = 0
				
				for row in result:
					members = row[0][1:-1].split(",")
					
					try:
						current_values = valid_patterns[','.join(members[0:len(members)-1])]
						current_values.append(members[-1])
						valid_patterns[','.join(members[0:len(members)-1])] = current_values
						valid_patterns_count =  valid_patterns_count + 1
					except KeyError, e:
						tmp = []
						tmp.append(members[-1])
						valid_patterns[','.join(members[0:len(members)-1])] = tmp
						valid_patterns_count =  valid_patterns_count + 1
						
				result = dbHandler.select("SELECT DISTINCT pattern_members FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = 2"  % (user))
							
				valid_patterns_2 = {}
				
				for row in result:
					members = row[0][1:-1].split(",")
					
					try:
						current_values = valid_patterns_2[','.join(members[0:1])]
						current_values.append(members[-1])
						valid_patterns_2[','.join(members[0:1])] = current_values
					except KeyError, e:
						tmp = []
						tmp.append(members[-1])
						valid_patterns_2[','.join(members[0:1])] = tmp
							
				result = dbHandler.select("SELECT * FROM Pattern_Base_%i WHERE count > 3 AND pattern_length = %i" % (user,pattern_length-1))
				
				last_pattern = ""
				
				current_pattern_temp_intervals = []
				current_pattern_temp_specificity = []
				current_sp_count = []
				
				test_patterns = []
				
				in_duration_group = 0
				
				for row in result:
					
					if last_pattern == "":
						last_pattern = row[3]
						current_pattern_temp_intervals.append(row[5])
						current_pattern_temp_specificity.append(row[9])
						current_sp_count.append(int(row[8]))
					else:
						if last_pattern == row[3]:
							current_pattern_temp_intervals.append(row[5])
							current_pattern_temp_specificity.append(row[9])
							current_sp_count.append(int(row[8]))
						else:
							try:
								members = last_pattern[1:-1].split(",")
								value_index = ','.join(members[1:len(members)])
								
								current_values = valid_patterns[value_index]
								
								for value in current_values:
									
									if value in valid_patterns_2[','.join(members[0:1])]:
										members = last_pattern[1:-1].split(",")
										members.append(value)
										pattern_combinations.append(members)
										pattern_combinations_temporal_intervals.append(current_pattern_temp_intervals)
										pattern_combinations_temporal_specificity.append(current_pattern_temp_specificity)
										pattern_combinations_count.append(current_sp_count)
								
							except KeyError, e:
								error = ""
							
							current_pattern_temp_intervals = []
							current_pattern_temp_specificity = []
							current_sp_count = []
							
							last_pattern = row[3]
							current_pattern_temp_intervals.append(row[5])
							current_pattern_temp_specificity.append(row[9])
							current_sp_count.append(int(row[8]))
											
		threads = []				
		
		max_pattern = len(pattern_combinations)		
		
		start = time.time()
		
		max_prob_calc = 0
				
		for temp_intervals in pattern_combinations_temporal_intervals:
			max_prob_calc = max_prob_calc + len(temp_intervals)
			
		num_threads = 3
			
		if max_prob_calc < 50000000 and max_prob_calc > 0:			
			print "Start calculating probabilities!!!"
			
			for k in range(num_threads):			
				if k != num_threads-1:
					lower = int((k)*math.floor(float(max_pattern)/float(num_threads)))
					upper = int((k+1)*math.floor(float(max_pattern)/float(num_threads)))
				else:
					lower = int((k)*math.floor(float(max_pattern)/float(num_threads)))
					upper = max_pattern
			
				if pattern_length > 2:
					probability_thread = pool.apply_async(Calculate_Probability, (pattern_combinations[lower:upper],pattern_combinations_temporal_intervals[lower:upper],pattern_combinations_temporal_specificity[lower:upper],pattern_combinations_count[lower:upper],pattern_length,user,0,len(pattern_combinations[lower:upper]),months,weeks,dates,timestamps,temporal_interval_description,) )
				else:					
					probability_thread = pool.apply_async(Calculate_Probability_Length2, (pattern_combinations[lower:upper],pattern_combinations_temporal_intervals[lower:upper],pattern_combinations_temporal_specificity[lower:upper],pattern_combinations_count[lower:upper],pattern_length,user,0,len(pattern_combinations[lower:upper]),months,weeks,dates,timestamps,temporal_interval_description,singular_pattern_id,) )
				threads.append(probability_thread)				
						
			for current_thread in threads:
				current_thread.get()				
				
			print time.time() - start
		else:
			error_log = open("error_log.txt","a")
			error_log.write("Stopped for %i at pattern length %i; Probability count: %i\n" % (user,pattern_length,max_prob_calc))
			error_log.close()
			
			break		
	
def Calculate_Probability(pattern_combinations,pattern_combinations_temporal_intervals,pattern_combinations_temporal_specificity,pattern_combinations_count,pattern_length,user,lower,upper,months,weeks,dates,timestamps,temporal_interval_description):
	
	print "		Calculate Probabilities for user %i from pattern %s to pattern %s (%i,%i)" % (user,pattern_combinations[lower],pattern_combinations[upper-1],lower,upper)
	
	dbHandler_pattern = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Instances")
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	fields = ["user_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	valid_instances = []
	gc.collect()

	valid_instances = Get_Valid_Instances(pattern_length-1,user)
	#dbHandler.truncateTable("Valid_Instances")
	
	instances_bulk = []
	bulk_insert = []
	
	for l in range(lower,upper):
		
		if len(instances_bulk) >= 5000:
			dbHandler_pattern.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			del instances_bulk
			instances_bulk = []
				
		pattern_comb = pattern_combinations[l]
		pattern_intervals = pattern_combinations_temporal_intervals[l]
		pattern_specificities = pattern_combinations_temporal_specificity[l]
		
		counts = numpy.zeros(7,int)
		
		start = time.time()
		
		for m in range(len(pattern_intervals)):
			count_index = temporal_interval_description.index(pattern_intervals[m])
			conditional_instances = []
	
			if count_index == 0:
				
				instances = valid_instances[0]
				
				if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
					instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
					instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
					
					x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
					
					sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
					
					sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]										
					
					month_instances = [v for j,v in enumerate(sub_valid) if all(months[int(pattern_comb[k-1])-1][v[k-1]] == months[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
					month_count = 0
					last_element = -1
					
					final_instances = []
					
					for instance in month_instances:
						if last_element == -1:
							month_count = month_count + 1
							last_element = instance[0]
							final_instances.append(instance)
						else:
							if last_element != instance[0]:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
					
					#instances[','.join(map(str,pattern_comb))] = final_instances
					#valid_instances[0] = instances
					conditional_instances = final_instances					
					counts[0] = month_count				
			else:		
				if count_index == 1:
					
					instances = valid_instances[1]
				
					if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
						instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
						instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
						
						x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
						
						sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
						
						sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
						
						month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 2 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
						month_count = 0
						last_element = -1
						
						final_instances = []
					
						for instance in month_instances:
							if last_element == -1:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
							else:
								if last_element != instance[0]:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
						
						#instances[','.join(map(str,pattern_comb))] = final_instances
						#valid_instances[1] = instances
						conditional_instances = final_instances
						counts[1] = month_count
				else:
					if count_index == 2:
						
						instances = valid_instances[2]
				
						if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
							instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
							instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
							
							x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
							
							sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
							
							sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
							
							month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 1 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
							month_count = 0
							last_element = -1
							
							final_instances = []
					
							for instance in month_instances:
								if last_element == -1:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
								else:
									if last_element != instance[0]:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
							
							#instances[','.join(map(str,pattern_comb))] = final_instances
							#valid_instances[2] = instances
							conditional_instances = final_instances
							counts[2] = month_count
					else:
						if count_index == 3:
							
							instances = valid_instances[3]
				
							if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
								instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
								instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
								
								x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
								
								sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
								
								sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
								
								month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[k-1])-1][v[k-1]] == weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
								month_count = 0
								last_element = -1
								
								final_instances = []
					
								for instance in month_instances:
									if last_element == -1:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
									else:
										if last_element != instance[0]:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
								
								#instances[','.join(map(str,pattern_comb))] = final_instances
								#valid_instances[3] = instances
								conditional_instances = final_instances
								counts[3] = month_count
						else:
							if count_index == 4:
								
								instances = valid_instances[4]
				
								if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
									instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
									instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
									
									x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
									
									sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
									
									sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
									
									month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 2*86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
									month_count = 0
									last_element = -1
									
									final_instances = []
					
									for instance in month_instances:
										if last_element == -1:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
										else:
											if last_element != instance[0]:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
									
									#instances[','.join(map(str,pattern_comb))] = final_instances
									#valid_instances[4] = instances
									conditional_instances = final_instances
									counts[4] = month_count
							else:
								if count_index == 5:
									
									instances = valid_instances[5]
				
									if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
										instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
										instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
										
										x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
										
										sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
										
										sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
										
										month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
										month_count = 0
										last_element = -1
										
										final_instances = []
					
										for instance in month_instances:
											if last_element == -1:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
											else:
												if last_element != instance[0]:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
										
										#instances[','.join(map(str,pattern_comb))] = final_instances
										#valid_instances[5] = instances
										conditional_instances = final_instances
										counts[5] = month_count
								else:
									if count_index == 6:
										
										instances = valid_instances[6]
				
										if instances.has_key(','.join(map(str,pattern_comb[0:-1]))) and instances.has_key(','.join(map(str,pattern_comb[1:len(pattern_comb)]))):
											instances_1 = instances[','.join(map(str,pattern_comb[0:-1]))]
											instances_2 = instances[','.join(map(str,pattern_comb[1:len(pattern_comb)]))]
											
											x = [(v,k) for j,v in enumerate(instances_1) for k in range(len(instances_2)) if v[-1] == instances_2[k][0]]
											
											sub_perm = [list(v[0]) + [instances_2[v[1]][-1]] for j,v in enumerate(x)]
											
											sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
											
											month_instances = [v for j,v in enumerate(sub_valid) if all(dates[int(pattern_comb[k-1])-1][v[k-1]] == dates[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
											month_count = 0
											last_element = -1
											
											final_instances = []
					
											for instance in month_instances:
												if last_element == -1:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
												else:
													if last_element != instance[0]:
														month_count = month_count + 1
														last_element = instance[0]
														final_instances.append(instance)
											
											#instances[','.join(map(str,pattern_comb))] = final_instances
											#valid_instances[6] = instances
											conditional_instances = final_instances
											counts[6] = month_count
										
			
			conditional_count = counts[count_index]
			
			if conditional_count > 0:
			
				probability = float(conditional_count)/pattern_combinations_count[l][m]
				
				pattern_description = ","
				
				for k in range(len(pattern_comb)):
					pattern_description = "%s%s," % (pattern_description,pattern_comb[k])
					
				pattern_condition = ","
				
				for k in range(len(pattern_comb)-1):
					pattern_condition = "%s%s," % (pattern_condition,pattern_comb[k])
				
				values = []
				values.append(user)				
				values.append(pattern_description)
				values.append(pattern_length)
				values.append(pattern_intervals[m])
				values.append(pattern_condition)
				values.append(probability)
				values.append(conditional_count)
				values.append(pattern_specificities[m])
				
				bulk_insert.append(values)
				
				values = []
				values.append(pattern_length)
				values.append(count_index)
				values.append(','.join(map(str,pattern_comb)))
				values.append(';'.join(map(To_String,conditional_instances)))
				instances_bulk.append(values)
			else:
				break		
			
		if len(bulk_insert) >= 5000:
			dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
			del bulk_insert
			bulk_insert = []
			
	dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
	dbHandler_pattern.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
		
	del bulk_insert
	del instances_bulk
	
	
def Calculate_Probability_Length2(pattern_combinations,pattern_combinations_temporal_intervals,pattern_combinations_temporal_specificity,pattern_combinations_count,pattern_length,user,lower,upper,months,weeks,dates,timestamps,temporal_interval_description,singular_pattern_id):
	
	print "		Calculate Probabilities for user %i from pattern %s to pattern %s (%i,%i)" % (user,pattern_combinations[lower],pattern_combinations[upper-1],lower,upper)
	
	dbHandler_pattern = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Instances")
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	fields = ["user_id","pattern_members","pattern_length","temporal_interval","pattern_condition","probability","count","specificity"]
	
	valid_instances = []
	
	for k in range(7):
		tmp = {}
		
		for sp_pattern_id in singular_pattern_id:
			tmp["%i" % (sp_pattern_id)] = range(len(timestamps[sp_pattern_id-1]))			
			
		valid_instances.append(tmp)
	
	instances_bulk = []
	bulk_insert = []
	
	for l in range(lower,upper):
		
		if len(instances_bulk) >= 10000:
			dbHandler_pattern.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			del instances_bulk
			instances_bulk = []
				
		pattern_comb = pattern_combinations[l]
		pattern_intervals = pattern_combinations_temporal_intervals[l]
		pattern_specificities = pattern_combinations_temporal_specificity[l]
		
		counts = numpy.zeros(7,int)
		
		for m in range(len(pattern_intervals)):
			count_index = temporal_interval_description.index(pattern_intervals[m])
			conditional_instances = []
	
			if count_index == 0:
				
				instances = valid_instances[0]
				
				if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
					instances_1 = instances['%s' % (pattern_comb[0])]
					instances_2 = instances['%s' % (pattern_comb[1])]
					
					sub_perm = []
					sub_perm.append(instances_1)
					sub_perm.append(instances_2)
					
					sub_perm = list(itertools.product(*sub_perm))
					sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]										
					
					month_instances = [v for j,v in enumerate(sub_valid) if all(months[int(pattern_comb[k-1])-1][v[k-1]] == months[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
					month_count = 0
					last_element = -1
					
					final_instances = []
					
					for instance in month_instances:
						if last_element == -1:
							month_count = month_count + 1
							last_element = instance[0]
							final_instances.append(instance)
						else:
							if last_element != instance[0]:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
					
					#instances[','.join(map(str,pattern_comb))] = final_instances
					#valid_instances[0] = instances
					conditional_instances = final_instances				
					counts[0] = month_count				
			else:		
				if count_index == 1:
					
					instances = valid_instances[1]
				
					if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
						instances_1 = instances['%s' % (pattern_comb[0])]
						instances_2 = instances['%s' % (pattern_comb[1])]
						
						sub_perm = []
						sub_perm.append(instances_1)
						sub_perm.append(instances_2)
						
						sub_perm = list(itertools.product(*sub_perm))
						sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
						
						month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 2 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
						month_count = 0
						last_element = -1
						
						final_instances = []
					
						for instance in month_instances:
							if last_element == -1:
								month_count = month_count + 1
								last_element = instance[0]
								final_instances.append(instance)
							else:
								if last_element != instance[0]:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
						
						#instances[','.join(map(str,pattern_comb))] = final_instances
						#valid_instances[1] = instances
						conditional_instances = final_instances
						counts[1] = month_count
				else:
					if count_index == 2:
						
						instances = valid_instances[2]
				
						if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
							instances_1 = instances['%s' % (pattern_comb[0])]
							instances_2 = instances['%s' % (pattern_comb[1])]
							
							sub_perm = []
							sub_perm.append(instances_1)
							sub_perm.append(instances_2)
							
							sub_perm = list(itertools.product(*sub_perm))
							sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
							
							month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[0])-1][v[0]] + 1 >= weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
							month_count = 0
							last_element = -1
							
							final_instances = []
					
							for instance in month_instances:
								if last_element == -1:
									month_count = month_count + 1
									last_element = instance[0]
									final_instances.append(instance)
								else:
									if last_element != instance[0]:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
							
							#instances[','.join(map(str,pattern_comb))] = final_instances
							#valid_instances[2] = instances
							conditional_instances = final_instances
							counts[2] = month_count
					else:
						if count_index == 3:
							
							instances = valid_instances[3]
				
							if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
								instances_1 = instances['%s' % (pattern_comb[0])]
								instances_2 = instances['%s' % (pattern_comb[1])]
								
								sub_perm = []
								sub_perm.append(instances_1)
								sub_perm.append(instances_2)
								
								sub_perm = list(itertools.product(*sub_perm))
								sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
								
								month_instances = [v for j,v in enumerate(sub_valid) if all(weeks[int(pattern_comb[k-1])-1][v[k-1]] == weeks[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
								month_count = 0
								last_element = -1
								
								final_instances = []
					
								for instance in month_instances:
									if last_element == -1:
										month_count = month_count + 1
										last_element = instance[0]
										final_instances.append(instance)
									else:
										if last_element != instance[0]:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
								
								#instances[','.join(map(str,pattern_comb))] = final_instances
								#valid_instances[3] = instances
								conditional_instances = final_instances
								counts[3] = month_count
						else:
							if count_index == 4:
								
								instances = valid_instances[4]
				
								if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
									instances_1 = instances['%s' % (pattern_comb[0])]
									instances_2 = instances['%s' % (pattern_comb[1])]
									
									sub_perm = []
									sub_perm.append(instances_1)
									sub_perm.append(instances_2)
									
									sub_perm = list(itertools.product(*sub_perm))
									sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
									
									month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 2*86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
									month_count = 0
									last_element = -1
									
									final_instances = []
					
									for instance in month_instances:
										if last_element == -1:
											month_count = month_count + 1
											last_element = instance[0]
											final_instances.append(instance)
										else:
											if last_element != instance[0]:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
									
									#instances[','.join(map(str,pattern_comb))] = final_instances
									#valid_instances[4] = instances
									conditional_instances = final_instances
									counts[4] = month_count
							else:
								if count_index == 5:
									
									instances = valid_instances[5]
				
									if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
										instances_1 = instances['%s' % (pattern_comb[0])]
										instances_2 = instances['%s' % (pattern_comb[1])]
										
										sub_perm = []
										sub_perm.append(instances_1)
										sub_perm.append(instances_2)
										
										sub_perm = list(itertools.product(*sub_perm))
										sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
										
										month_instances = [v for j,v in enumerate(sub_valid) if all(timestamps[int(pattern_comb[0])-1][v[0]] + 86400 >= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
										month_count = 0
										last_element = -1
										
										final_instances = []
					
										for instance in month_instances:
											if last_element == -1:
												month_count = month_count + 1
												last_element = instance[0]
												final_instances.append(instance)
											else:
												if last_element != instance[0]:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
										
										#instances[','.join(map(str,pattern_comb))] = final_instances
										#valid_instances[5] = instances
										conditional_instances = final_instances
										counts[5] = month_count
								else:
									if count_index == 6:
										
										instances = valid_instances[6]
				
										if instances.has_key('%s' % (pattern_comb[0])) and instances.has_key('%s' % (pattern_comb[1])):
											instances_1 = instances['%s' % (pattern_comb[0])]
											instances_2 = instances['%s' % (pattern_comb[1])]
											
											sub_perm = []
											sub_perm.append(instances_1)
											sub_perm.append(instances_2)
											
											sub_perm = list(itertools.product(*sub_perm))
											sub_valid = [v for j,v in enumerate(sub_perm) if all(timestamps[int(pattern_comb[k-1])-1][v[k-1]] <= timestamps[int(pattern_comb[k])-1][v[k]] for k in range(1,2))]
											
											month_instances = [v for j,v in enumerate(sub_valid) if all(dates[int(pattern_comb[k-1])-1][v[k-1]] == dates[int(pattern_comb[k])-1][v[k]] for k in range(1,len(pattern_comb)))]
			
											month_count = 0
											last_element = -1
											
											final_instances = []
					
											for instance in month_instances:
												if last_element == -1:
													month_count = month_count + 1
													last_element = instance[0]
													final_instances.append(instance)
												else:
													if last_element != instance[0]:
														month_count = month_count + 1
														last_element = instance[0]
														final_instances.append(instance)
											
											#instances[','.join(map(str,pattern_comb))] = final_instances
											#valid_instances[6] = instances
											conditional_instances = final_instances
											counts[6] = month_count
										
			
			conditional_count = counts[count_index]
			
			if conditional_count > 0:
			
				probability = float(conditional_count)/pattern_combinations_count[l][m]
				
				pattern_description = ","
				
				for k in range(len(pattern_comb)):
					pattern_description = "%s%s," % (pattern_description,pattern_comb[k])
					
				pattern_condition = ","
				
				for k in range(len(pattern_comb)-1):
					pattern_condition = "%s%s," % (pattern_condition,pattern_comb[k])
				
				values = []
				values.append(user)				
				values.append(pattern_description)
				values.append(pattern_length)
				values.append(pattern_intervals[m])
				values.append(pattern_condition)
				values.append(probability)
				values.append(conditional_count)
				values.append(pattern_specificities[m])
				
				bulk_insert.append(values)
				
				values = []
				values.append(pattern_length)
				values.append(count_index)
				values.append(','.join(map(str,pattern_comb)))
				values.append(';'.join(map(To_String,conditional_instances)))
				instances_bulk.append(values)
			else:
				break		
			
		if len(bulk_insert) >= 10000:
			dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
			del bulk_insert
			bulk_insert = []
			
	dbHandler.insert_bulk("Pattern_Base_%i" % (user),fields,bulk_insert)
	dbHandler_pattern.insert_bulk("Valid_Instances_%i" % (user),['pattern_length','temporal_interval','pattern','instances'],instances_bulk)
			
	del bulk_insert
	del instances_bulk
	

# ================================= Begin of Support Methods =================================

def Get_Interval_Name(index):
	
	if index == 0:
		return "same"
	
	if index == 1:
		return "adjacent_1"
	
	if index > 1 and index % 2 == 0:
		return "at_adjacent_%i" % (index/2 + 1)
	
	if index > 1 and index % 2 != 0:
		return "within_adjacent_%i" % ((index-1)/2 + 1)

def Probability_Count(doy,search_list,search_element):
	
	last_doy = -1
	search_count = 0
	
	for i in range(len(doy)):
		
		if last_doy != doy[i]:						
			if search_list[i] == search_element:
				search_count = search_count + 1
			
		last_doy = doy[i]
		
	return search_count
	
def Find_Elements(search_list,element):
	
	search_indices = []
	
	for i in range(len(search_list)):
		if search_list[i] == element:
			search_indices.append(i)
			
	return search_indices
	
def Get_Member_List(event_ids,reps):
	
	output = ""
	
	for i in range(len(reps)-1):
		output = "%s%i," % (output,event_ids[reps[i]])
		
	output = "%s%i" % (output,event_ids[reps[-1]])
	
	return output


def Get_Cluster_ID(activity,location,cluster_index):
	
	if activity == 'Transition-To':
		return "TT_%i_%i" % (location,cluster_index)

	if activity == 'Transition-From':
		return "TF_%i_%i" % (location,cluster_index)
	
	if activity == 'Duration':
		return "D_%i_%i" % (location,cluster_index)
	
	if activity == 'First Arrival':
		return "FA_%i_%i" % (location,cluster_index)
	
	if activity == 'Last Departure':
		return "LD_%i_%i" % (location,cluster_index)
	

def Find_Best_Clustering(user,activity):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	result = dbHandler.select("SELECT outer_temporal_threshold,inner_temporal_threshold,(number_short_clusters/number_clusters) as cluster_length_coef FROM Pattern_Analysis WHERE activity = '%s' AND user_id = %i ORDER BY dunn_index DESC LIMIT 5" % (activity,user))
	
	outer_thresholds = []
	inner_thresholds = []
	cluster_coef = []
	
	for row in result:
		outer_thresholds.append(int(row[0]))
		inner_thresholds.append(int(row[1]))
		cluster_coef.append(float(row[2]))
		
	lowest = numpy.argmin(numpy.array(cluster_coef))
	
	return outer_thresholds[lowest]/5,inner_thresholds[lowest]/5
		

def Calculate_Cluster_Density(means):
	
	# Calculates distance of all points to the mean
	
	distances = []
	cluster_mean = numpy.mean(numpy.array(means))
	
	for i in range(len(means)):		
		distances.append(math.fabs(means[i]-cluster_mean))
			
	
	return float(numpy.sum(numpy.array(distances)))/float(len(means))
	
def Find_Nearest(array,value):
    idx = (numpy.abs(array-value)).argmin()
    return idx

def Calculate_Pattern_Coverage(user,length):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	distinct_count = []
	
	result = dbHandler.select("SELECT DISTINCT count FROM Pattern_Base WHERE user_id = %i AND pattern_length = %i ORDER BY count DESC" % (user,length))
	
	for row in result:
		distinct_count.append(int(row[0]))
		
	distinct_patterns = []
	
	for k in range(distinct_count[0]):
		distinct_patterns.append([])
	
	result = dbHandler.select("SELECT count,pattern_members FROM Pattern_Base WHERE user_id = %i AND pattern_length = %i" % (user,length))
	
	for row in result:
		distinct_patterns[int(row[0])-1].append(row[1][1:-1])		
			
	distinct_patterns_tmp = []
	
	for pattern_collection in distinct_patterns:
		tmp = list(set(pattern_collection))
		
		sp_tmp = []
		
		for pattern in tmp:
			members = pattern.split(",")
			
			for k in range(len(members)):
				sp_tmp.append(int(members[k]))		
		
		distinct_patterns_tmp.append(list(set(sp_tmp)))
		
	result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	max_sp_id = 0
	
	for row in result:
		max_sp_id = int(row[0])
	
	singular_patterns = []
	
	for k in range(max_sp_id):
		singular_patterns.append([])
	
	result = dbHandler.select("SELECT pattern_id,members FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (user))
	
	for row in result:		
		singular_patterns[int(row[0])-1] = row[1][1:-1].split(",")
		
	event_list = []
		
	for patterns in distinct_patterns_tmp:
		
		events = []
		
		for k in range(len(patterns)):
			events_tmp = singular_patterns[patterns[k]-1]
			
			for l in range(len(events_tmp)):
				events.append(events_tmp[l])
				
		event_list.append(events)
		
	result = dbHandler.select("SELECT max(event_id) FROM Events_GHC_final WHERE user_id = %i" % (user))
	
	max_event_id = 0
	
	for row in result:
		max_event_id = int(row[0])
		
	coverage = []
	
	for events in event_list:
		coverage.append(float(len(list(set(events))))/float(max_event_id))
		
	print coverage
	
	total_list = []
	
	for k in range(3,len(event_list)):
		for l in range(len(event_list[k])):
			total_list.append(event_list[k][l])
			
	print float(len(list(set(total_list))))/float(max_event_id)
	

def To_String(inst):
	
	output = ""
	
	for i in range(len(inst)-1):
		output = "%s%s," % (output,inst[i])
		
	output = "%s%s" % (output,inst[-1])
	
	return output

def Get_Valid_Instances(pattern_length,user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Instances")
	result = dbHandler.select("SELECT * FROM Valid_Instances_%i WHERE pattern_length = %i" % (user,pattern_length))
	
	valid_instances = []
	
	for k in range(7):
		valid_instances.append({})
		
	for row in result:
		valid_instances[int(row[2])][row[3]] = [map(int,v.split(",")) for j,v in enumerate(row[4].split(";"))]
		
	return valid_instances	



# ================================= End of Support Methods =================================

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	user = int(sys.argv[3])
	
	max_interval_length = 2
	
	#Extract_Conditional_Patterns(2,max_interval_length)
		
	"""
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	result = dbHandler.select("SELECT DISTINCT user_id FROM Data_GHC_final WHERE user_id > 18")
	
	users = []
	
	for row in result:
		users.append(int(row[0]))
		
	"""
	pool = Pool(3)
		
	Extract_Conditional_Patterns(user,max_interval_length,pool)
	
	#Extract_Conditional_Patterns(8,max_interval_length,pool)
	
	#Calculate_Pattern_Coverage(2,4)
	
	