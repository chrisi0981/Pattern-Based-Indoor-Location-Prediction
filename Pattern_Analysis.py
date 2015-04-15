#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Analyze_Patterns():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
	
	result = dbHandler.select("SELECT DISTINCT user_id FROM Singular_Pattern_Base")
	
	user_ids = []
	
	for row in result:
		user_ids.append(int(row[0]))
		
	for user in user_ids:
		
		result = dbHandler.select("SELECT pattern_length,count(id) FROM Pattern_Base_%i GROUP by pattern_length ORDER BY pattern_length" % (user))
		
		for row in result:
			
			values = []
			values.append(user)
			values.append(int(row[0]))
			values.append(int(row[1]))
			dbHandler.insert("Analysis",["user_id","pattern_length","number_patterns"],values)
	
	

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	
	Analyze_Patterns()