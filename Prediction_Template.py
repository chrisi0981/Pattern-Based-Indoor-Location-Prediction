#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys

import time
import datetime
from pytz import timezone

from numpy.random import random_sample
import numpy
import scipy.io

import sklearn.linear_model as linMod
from sklearn import linear_model
from sklearn import svm
from sklearn import neighbors
from sklearn import naive_bayes
from sklearn import tree
from sklearn import qda
from sklearn.decomposition import PCA
import sklearn
from sklearn.metrics.pairwise import euclidean_distances
from sklearn import gaussian_process
from sklearn.hmm import MultinomialHMM
from sklearn.ensemble import GradientBoostingClassifier
from sklearn import mixture
from sklearn import cluster

import itertools
        
        
def Run_PerceptronPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
        
        perceptron_model = sklearn.linear_model.Perceptron(penalty=None, alpha=0.0001, fit_intercept=True, n_iter=5, shuffle=False, verbose=0, eta0=1.0, n_jobs=1, random_state=0, class_weight=None, warm_start=False)
        perceptron_model.fit(training_feature_matrix, training_ground_truth)
        
        return perceptron_model.predict(test_feature_matrix)
        
        
def Run_LRPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
            
        lr_model = sklearn.linear_model.LogisticRegression(penalty='l1', dual=False, tol=0.0001, C=1.0, fit_intercept=True, intercept_scaling=1, class_weight=None, random_state=None)
        lr_model.fit(training_feature_matrix, training_ground_truth)
        
        return lr_model.predict(test_feature_matrix)
            
        
def Run_KNNPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
        
        knn_model = sklearn.neighbors.KNeighborsClassifier(n_neighbors=3, weights='uniform', algorithm='auto', leaf_size=30, p=3, metric='minkowski')
        knn_model.fit(training_feature_matrix, training_ground_truth)
        
        return knn_model.predict(test_feature_matrix)
        
def Run_DecisionTreePredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
        
        dt_model = sklearn.tree.DecisionTreeClassifier()
        dt_model.fit(training_feature_matrix, training_ground_truth)
        
        return dt_model.predict(test_feature_matrix)
        
def Run_GradientBoostPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
            
        gradientBoost_model = GradientBoostingClassifier()
        gradientBoost_model.fit(training_feature_matrix, training_ground_truth)
        
        return gradientBoost_model.predict(test_feature_matrix)
        
def Run_GaussianNaiveBayesPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
            
        gnb_model = naive_bayes.GaussianNB()
        gnb_model.fit(training_feature_matrix, training_ground_truth)
        
        return gnb_model.predict(test_feature_matrix)
        
def Run_SVMPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
            
        SVM_model = svm.SVC(C=1.0, cache_size=200, class_weight=None, coef0=0.0, degree=2, gamma=0.0, kernel='rbf', max_iter=-1, probability=False, random_state=None, shrinking=True, tol=0.001, verbose=False)
        SVM_model.fit(training_feature_matrix, training_ground_truth) 
        
        return SVM_model.predict(test_feature_matrix)
        
def Run_QDAPredictor(training_feature_matrix, test_feature_matrix, training_ground_truth):
                
        qda_model = qda.QDA()
        qda_model.fit(training_feature_matrix, training_ground_truth) 
        
        return qda_model.predict(test_feature_matrix)
        
def Predict_Pain_All_Features():
        
        dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Nokia")
        
        test_dates = []
        
        result = dbHandler.select("SELECT DISTINCT date FROM Pain_Feature_Matrix ORDER BY date")
        
        dates = []
        
        for row in result:
                dates.append(row[0])
                
        for k in range(int(math.floor(len(dates)/2)),len(dates)):
        
                #print "SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date < '%s'" % (dates[k])
        
                result = dbHandler.select("SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date < '%s'" % (dates[k]))
                
                training_labels = []
                training_feature_set = []
                
                for row in result:
                    training_labels.append(int(row[0]))
                    
                    values = []                
                    values.append(int(row[1]))
                    values.append(int(row[2]))
                    values.append(int(row[3]))
                    values.append(int(row[4]))
                    
                    values.append(int(row[5]))
                    values.append(int(row[6]))
                    values.append(int(row[7]))
                    values.append(int(row[8]))
                    values.append(int(row[9]))
                    
                    training_feature_set.append(numpy.array(values))
                
                #print "SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date = '%s'" % (dates[k])
                
                result = dbHandler.select("SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date = '%s'" % (dates[k]))
                
                test_labels = []
                test_feature_set = []
                
                for row in result:
                    test_labels.append(int(row[0]))
                    
                    values = []                
                    values.append(int(row[1]))
                    values.append(int(row[2]))
                    values.append(int(row[3]))
                    values.append(int(row[4]))
                    
                    values.append(int(row[5]))
                    values.append(int(row[6]))
                    values.append(int(row[7]))
                    values.append(int(row[8]))
                    values.append(int(row[9]))
                    
                    test_feature_set.append(numpy.array(values))
                
                arguments = ['Lumoback_Activity','Time_Index','Weekday','Week_Weekend','Previous_Pain','Sit_Good','Sit_Bad','Walk','Stand']
                
                argument_selection = []
                
                for combination in itertools.combinations(range(4),1):
                        argument_selection.append(list(combination))
                for combination in itertools.combinations(range(4),2):
                        argument_selection.append(list(combination))
                for combination in itertools.combinations(range(4),3):
                        argument_selection.append(list(combination))
                for combination in itertools.combinations(range(4),4):
                        argument_selection.append(list(combination))
        
                full_training_feature_set = numpy.array(training_feature_set)
                full_test_feature_set = numpy.array(test_feature_set)
                arguments = numpy.array(arguments)
                
                for selection in argument_selection:
                        
                        training_feature_set = full_training_feature_set[:,selection]
                        test_feature_set = full_test_feature_set[:,selection]
                        
                        results = []
                        
                        results.append(Run_KNNPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        results.append(Run_GaussianNaiveBayesPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        results.append(Run_DecisionTreePredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        results.append(Run_PerceptronPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        results.append(Run_LRPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        results.append(Run_QDAPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                        
                        res = numpy.bincount(numpy.array(training_labels))
                        
                        majority_predictor = numpy.where(res==numpy.amax(res))[0][0]
                        
                        error_values = []
                       
                        
                        for i in range(len(test_labels)):
                                values = []
                                values.append(dates[k])
                                values.append('-'.join(list(arguments[selection])))
                                values.append(test_labels[i])
                                values.append(results[1][i])
                                values.append(results[0][i])
                                values.append(results[3][i])
                                values.append(results[2][i])
                                values.append(results[4][i])
                                values.append(results[5][i])
                                values.append(majority_predictor)
                                
                                result_fields = ['Date','Feature_Set','Ground_Truth','GNB','KNN','Perceptron','DT','LR','QDA','ZeroR']                        
                                dbHandler.insert("Pain_Prediction_Results",result_fields,values)
                                
def Predict_Pain_For_Feature_Set(selection):
        
        dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Nokia")
        
        test_dates = []
        
        result = dbHandler.select("SELECT DISTINCT date FROM Pain_Feature_Matrix_History ORDER BY date")
        
        dates = []
        
        for row in result:
                dates.append(row[0])
                
        for k in range(int(math.floor(len(dates)/2)),len(dates)):
        
                #print "SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date < '%s'" % (dates[k])
        
                result = dbHandler.select("SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Sit_Good_24,Sit_Bad_24,Walk_24,Stand_24,Sit_Good_48,Sit_Bad_48,Walk_48,Stand_48,Sit_Good_72,Sit_Bad_72,Walk_72,Stand_72 FROM Pain_Feature_Matrix_History WHERE date < '%s'" % (dates[k]))
                
                training_labels = []
                training_feature_set = []
                
                for row in result:
                    training_labels.append(int(row[0]))
                    
                    values = []                
                    values.append(int(row[1]))
                    values.append(int(row[2]))
                    values.append(int(row[3]))
                    values.append(int(row[4]))
                    
                    values.append(int(row[5]))
                    values.append(int(row[6]))
                    values.append(int(row[7]))
                    values.append(int(row[8]))
                    
                    values.append(int(row[9]))
                    values.append(int(row[10]))
                    values.append(int(row[11]))
                    values.append(int(row[12]))
                    
                    values.append(int(row[13]))
                    values.append(int(row[14]))
                    values.append(int(row[15]))
                    values.append(int(row[16]))
                    
                    training_feature_set.append(numpy.array(values))
                
                #print "SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Previous_Pain,Sit_Good,Sit_Bad,Walk,Stand FROM Pain_Feature_Matrix WHERE date = '%s'" % (dates[k])
                
                result = dbHandler.select("SELECT ground_truth,Lumoback_Activity,Time_Index,Weekday,Week_Weekend,Sit_Good_24,Sit_Bad_24,Walk_24,Stand_24,Sit_Good_48,Sit_Bad_48,Walk_48,Stand_48,Sit_Good_72,Sit_Bad_72,Walk_72,Stand_72 FROM Pain_Feature_Matrix_History WHERE date = '%s'" % (dates[k]))
                
                test_labels = []
                test_feature_set = []
                
                for row in result:
                    test_labels.append(int(row[0]))
                    
                    values = []                
                    values.append(int(row[1]))
                    values.append(int(row[2]))
                    values.append(int(row[3]))
                    values.append(int(row[4]))
                    
                    values.append(int(row[5]))
                    values.append(int(row[6]))
                    values.append(int(row[7]))
                    values.append(int(row[8]))
                    
                    values.append(int(row[9]))
                    values.append(int(row[10]))
                    values.append(int(row[11]))
                    values.append(int(row[12]))
                    
                    values.append(int(row[13]))
                    values.append(int(row[14]))
                    values.append(int(row[15]))
                    values.append(int(row[16]))
                    
                    test_feature_set.append(numpy.array(values))
                
                arguments = ['Lumoback_Activity','Time_Index','Weekday','Week_Weekend','Sit_Good_24','Sit_Bad_24','Walk_24','Stand_24','Sit_Good_48','Sit_Bad_48','Walk_48','Stand_48','Sit_Good_72','Sit_Bad_72','Walk_72','Stand_72']
                
                full_training_feature_set = numpy.array(training_feature_set)
                full_test_feature_set = numpy.array(test_feature_set)
                arguments = numpy.array(arguments)
                     
                training_feature_set = full_training_feature_set[:,selection]
                test_feature_set = full_test_feature_set[:,selection]
                
                results = []
                
                results.append(Run_KNNPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                results.append(Run_GaussianNaiveBayesPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                results.append(Run_DecisionTreePredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                results.append(Run_PerceptronPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                results.append(Run_LRPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                results.append(Run_QDAPredictor(numpy.array(training_feature_set),numpy.array(test_feature_set),numpy.array(training_labels)))
                
                res = numpy.bincount(numpy.array(training_labels))
                
                majority_predictor = numpy.where(res==numpy.amax(res))[0][0]
                
                error_values = []
               
                
                for i in range(len(test_labels)):
                        values = []
                        values.append(dates[k])
                        values.append('-'.join(list(arguments[selection])))
                        values.append(test_labels[i])
                        values.append(results[1][i])
                        values.append(results[0][i])
                        values.append(results[3][i])
                        values.append(results[2][i])
                        values.append(results[4][i])
                        values.append(results[5][i])
                        values.append(majority_predictor)
                        
                        result_fields = ['Date','Feature_Set','Ground_Truth','GNB','KNN','Perceptron','DT','LR','QDA','ZeroR']                        
                        dbHandler.insert("Pain_Prediction_Results_History",result_fields,values)
        
        
if __name__ == "__main__":
        
        #Predict_Pain_All_Features()
        #Predict_Pain_For_Feature_Set([0,4,5,6,7])
        Predict_Pain_For_Feature_Set([0,8,9,10,11])
        Predict_Pain_For_Feature_Set([0,12,13,14,15])
        Predict_Pain_For_Feature_Set([0,4,5,6,7,8,9,10,11])