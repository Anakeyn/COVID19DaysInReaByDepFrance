# -*- coding: utf-8 -*-
"""
Created on Mon Sep  13 16:28:24 2020

@author: Pierre
"""

import pandas as pd
import numpy as np
from datetime import datetime   #, timedelta, date 
import random
import statistics #for mean


#Define files with original data 
sursaudFile = "sursaud-corona-quot-dep-2020-09-14-19h15.csv"
hospFile = "donnees-hospitalieres-covid19-2020-09-14-19h00.csv"



######################################################################################
# I - create newHosp, newRea, newReaOut, newRad, newDc Tidy data dataframe/file
######################################################################################

###############################################################
# Read sursaud file :
# i.e sursaud-corona-quot-dep-YYYY-MM-DD-19h15.csv
###############################################################
#Import file with necessary variables
df = pd.read_csv(sursaudFile, sep=";" ,  
                         usecols= ["dep","date_de_passage","sursaud_cl_age_corona", "nbre_hospit_corona"],
                         dtype={"dep": str},  #force dep to string
                         parse_dates= ["date_de_passage"])
df.dtypes

df.rename(columns={"date_de_passage": "day" }, inplace=True)
df["nbre_hospit_corona"] = df["nbre_hospit_corona"].fillna(0)  #replace NaN by 0
#df['dep'].astype(str)  #force dep to string #done in import
df['dep'] = df['dep'].str.strip() #strip white spaces
#select only global data 
df = df.loc[df['sursaud_cl_age_corona'] == '0']
#remove age classe column
dfUrgences = df.drop(columns=['sursaud_cl_age_corona'])


#Reindex
dfUrgences.reset_index(inplace=True, drop=True)  #reset index

#add cumul column
dfUrgences['cumul_hospit_corona']=0.0
dfUrgences.dtypes


previousDep = "00"
previousCumul = 0.0

#calculate cumul_hospit_Corona

for index, row in dfUrgences.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        previousCumul = 0.0
        dfUrgences.loc[index, 'cumul_hospit_corona'] = row['nbre_hospit_corona']
        print("First line")
    else :
        dfUrgences.loc[index, 'cumul_hospit_corona'] = row['nbre_hospit_corona'] + previousCumul
        print("other lines")
    previousDep = row['dep']
    previousCumul = row['nbre_hospit_corona'] + previousCumul
    print("previousCumul:", previousCumul)
    
#Save Urgences data if needed
#dfUrgences.to_excel("Urgences.xlsx", sheet_name='Urgences', index=True)    
dfUrgences.dtypes




###############################################################
# Read Hospitalizations  file :
# i.e donnnees-hospitalieres-covid19-YYYY-MM-DD-19h00.csv
###############################################################
df = pd.read_csv(hospFile, sep=";" ,  
                         usecols= ["dep","sexe", "jour","hosp", "rea", "rad", "dc"],
                         dtype={"dep": str},
                         parse_dates= ["jour"])
df.dtypes

df.rename(columns={"jour": "day" }, inplace=True)
#df['dep'].astype(str)  #force dep to string #done in import
df['dep'] = df['dep'].str.strip() #strip white spaces
#select global data 
df = df.loc[df['sexe'] == 0]
#remove sex column
dfHospitalizations = df.drop(columns=['sexe'])
#Remove well known error dep = Na for 2020-03-24
dfHospitalizations.dropna(subset=['dep'],inplace=True)

#Sort it
dfHospitalizations.sort_values(['dep', 'day'], ascending=[True, True], inplace=True)

#Reindex
dfHospitalizations.reset_index(inplace=True, drop=True)  #reset index

#hospCumul = max previous value or hosp + rad + dc
dfHospitalizations["hospCumul"]=0.0
previousCumul=0.0

for index, row in dfHospitalizations.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        previousCumul = dfHospitalizations.loc[index, 'hospCumul'] = row["hosp"]+row["rad"]+row["dc"]
        print("First line")
    else :
        dfHospitalizations.loc[index, 'hospCumul'] = max(previousCumul, row["hosp"]+row["rad"]+row["dc"])
        previousCumul = dfHospitalizations.loc[index, 'hospCumul']
        print("other lines")
    previousDep = row['dep']
    print("previousCumul:", previousCumul)

#save hospitalizations if needed
#dfHospitalizations.to_excel("Hospitalizations.xlsx", sheet_name='Hospitalizations.xlsx', index=True)  
dfHospitalizations.dtypes


######################################################################
#  Merge Urgences and Hospitalizations based on Departements and days 
######################################################################
dfUrgHosp = pd.merge(dfUrgences, dfHospitalizations, on = ['dep', 'day'], how="left")

dfUrgHosp.dtypes

dfUrgHosp["hospCumul"] = dfUrgHosp["hospCumul"].fillna(0)  #remove NaN
dfUrgHosp["hosp"] = dfUrgHosp["hosp"].fillna(0)  #remove NaN 
dfUrgHosp["rea"] = dfUrgHosp["rea"].fillna(0)  #remove NaN 
dfUrgHosp["rad"] = dfUrgHosp["rad"].fillna(0)  #remove NaN 
dfUrgHosp["dc"] = dfUrgHosp["dc"].fillna(0)  #remove NaN 

#Resort it  day descending
dfUrgHosp.sort_values(by=['dep', 'day'], ascending=[True, False],  inplace=True)
#Reindex
dfUrgHosp.reset_index(inplace=True, drop=True)  #reset index



#usefull function
def nonZeroYbyX(x,y):
    if x>0 :
        return y/x
    else:
        return 0




###########################################################
# Calculate hospCumul vs cumul_hospit_corona factor
###########################################################

dfUrgHosp['hospCumul_cumul_hosp_corona_factor'] = 0.0
valuesForMean = []
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        valuesForMean = [] #new valuez for mean depending on dep
        dfUrgHosp.loc[index, 'hospCumul_cumul_hosp_corona_factor'] = nonZeroYbyX(row["cumul_hospit_corona"],row["hospCumul"])
        print("First line")
    else :
        if row['day'] > datetime(2020, 3, 17, 0, 0, 0) :
            dfUrgHosp.loc[index, 'hospCumul_cumul_hosp_corona_factor'] = nonZeroYbyX(row["cumul_hospit_corona"],row["hospCumul"])
            
        else :
            dfUrgHosp.loc[index, 'hospCumul_cumul_hosp_corona_factor'] = statistics.mean(valuesForMean[-10:])
    valuesForMean.append(dfUrgHosp.loc[index, 'hospCumul_cumul_hosp_corona_factor'])
    previousDep = row['dep']
    print("valuesForMean:", valuesForMean)


###########################################################
# ReCalculate hospCumul from 24/02 to today hospCumulRec
###########################################################

dfUrgHosp['hospCumulRec'] = 0.0
nextHospCumulRec = 0.0
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep) | (row['day'] > datetime(2020, 3, 17, 0, 0, 0)): #here began with the most recent day
        nextHospCumulRec = 0.0
        if row['hospCumul_cumul_hosp_corona_factor']>0 :
            dfUrgHosp.loc[index, 'hospCumulRec'] = round(row["cumul_hospit_corona"]*row['hospCumul_cumul_hosp_corona_factor'],0)
        else : 
            dfUrgHosp.loc[index, 'hospCumulRec'] = row['hospCumul']
        print("Most recents dates")
    else :  #before 17/03/20
        if row['hospCumul_cumul_hosp_corona_factor']>0  :
            dfUrgHosp.loc[index, 'hospCumulRec'] = min(nextHospCumulRec,round(row["cumul_hospit_corona"]*row['hospCumul_cumul_hosp_corona_factor'],0))
        else :
            dfUrgHosp.loc[index, 'hospCumulRec'] = row['hospCumul']
    nextHospCumulRec = dfUrgHosp.loc[index, 'hospCumulRec']
    previousDep = row['dep']
    print(" nextHospCumulRec:", nextHospCumulRec)



###########################################################
# rad : Retour à Domicile : Go back home
###########################################################
# Calculate Rad (Cumul) vs hospCumulRec factor
###########################################################

dfUrgHosp['rad_hospCumulRec_factor'] = 0.0
valuesForMean = []
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        valuesForMean = [] #new valuez for mean depending on dep
        dfUrgHosp.loc[index, 'rad_hospCumulRec_factor'] = nonZeroYbyX(row["hospCumulRec"],row["rad"])
        print("First line")
    else :
        if row['day'] > datetime(2020, 3, 17, 0, 0, 0) :
            dfUrgHosp.loc[index, 'rad_hospCumulRec_factor'] = nonZeroYbyX(row["hospCumulRec"],row["rad"])
            
        else :
            dfUrgHosp.loc[index, 'rad_hospCumulRec_factor'] = statistics.mean(valuesForMean[-10:])
    valuesForMean.append(dfUrgHosp.loc[index, 'rad_hospCumulRec_factor'])
    previousDep = row['dep']
    print("valuesForMean:", valuesForMean)


###########################################################
# ReCalculate rad (cumul) from 24/02 to today radCumulRec
###########################################################

dfUrgHosp['radCumulRec'] = 0.0
nextRadCumulRec = 0.0
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep) | (row['day'] > datetime(2020, 3, 17, 0, 0, 0)): #here began with the most recent day
        nextRadCumulRec = 0.0
        if row['rad_hospCumulRec_factor']>0 :
            dfUrgHosp.loc[index, 'radCumulRec'] = round(row["hospCumulRec"]*row['rad_hospCumulRec_factor'],0)
        else : 
            dfUrgHosp.loc[index, 'radCumulRec'] = row['rad']
        print("Most recents dates")
    else :  #before 17/03/20
        if row['rad_hospCumulRec_factor']>0  :
            dfUrgHosp.loc[index, 'radCumulRec'] = min(nextRadCumulRec,round(row["hospCumulRec"]*row['rad_hospCumulRec_factor'],0))
        else :
            dfUrgHosp.loc[index, 'radCumulRec'] = row['rad']
    nextRadCumulRec = dfUrgHosp.loc[index, 'radCumulRec']
    previousDep = row['dep']
    print(" nextRadCumulRec:", nextRadCumulRec)

###########################################################
# dc : décès : Dead
###########################################################
# Calculate dc (Cumul) vs hospCumulRec factor
###########################################################

dfUrgHosp['dc_hospCumulRec_factor'] = 0.0
valuesForMean = []
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        valuesForMean = [] #new valuez for mean depending on dep
        dfUrgHosp.loc[index, 'dc_hospCumulRec_factor'] = nonZeroYbyX(row["hospCumulRec"],row["dc"])
        print("First line")
    else :
        if row['day'] > datetime(2020, 3, 17, 0, 0, 0) :
            dfUrgHosp.loc[index, 'dc_hospCumulRec_factor'] = nonZeroYbyX(row["hospCumulRec"],row["dc"])
            
        else :
            dfUrgHosp.loc[index, 'dc_hospCumulRec_factor'] = statistics.mean(valuesForMean[-10:])
    valuesForMean.append(dfUrgHosp.loc[index, 'dc_hospCumulRec_factor'])
    previousDep = row['dep']
    print("valuesForMean:", valuesForMean)


###########################################################
# ReCalculate dc (cumul) from 24/02 to today dcCumulRec
###########################################################

dfUrgHosp['dcCumulRec'] = 0.0
nextdcCumulRec = 0.0
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep) | (row['day'] > datetime(2020, 3, 17, 0, 0, 0)): #here began with the most recent day
        nextdcCumulRec = 0.0
        if row['dc_hospCumulRec_factor']>0 :
            dfUrgHosp.loc[index, 'dcCumulRec'] = round(row["hospCumulRec"]*row['dc_hospCumulRec_factor'],0)
        else : 
            dfUrgHosp.loc[index, 'dcCumulRec'] = row['dc']
        print("Most recents dates")
    else :  #before 17/03/20
        if row['dc_hospCumulRec_factor']>0  :
            dfUrgHosp.loc[index, 'dcCumulRec'] = min(nextdcCumulRec,round(row["hospCumulRec"]*row['dc_hospCumulRec_factor'],0))
        else :
            dfUrgHosp.loc[index, 'dcCumulRec'] = row['dc']
    nextdcCumulRec = dfUrgHosp.loc[index, 'dcCumulRec']
    previousDep = row['dep']
    print(" nextdcCumulRec:", nextdcCumulRec)


################################################################
# Recalculate Hosp Stock one day : hospDRec with an apply 
################################################################
dfUrgHosp["hospDRec"]  = dfUrgHosp.apply(lambda x: (x["hospCumulRec"] -  x["radCumulRec"] -  x["dcCumulRec"]), axis=1) 


###########################################################
# reaDRec : recalculate rea (stock one day)
###########################################################
# Calculate rea (stock) vs hospDRec  (stock) factor
###########################################################

dfUrgHosp['rea_hospDrec_factor'] = 0.0
valuesForMean = []
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        valuesForMean = [] #new valuez for mean depending on dep
        dfUrgHosp.loc[index, 'rea_hospDrec_factor'] = nonZeroYbyX(row["hospDRec"],row["rea"])
        print("First line")
    else :
        if row['day'] > datetime(2020, 3, 17, 0, 0, 0) :
            dfUrgHosp.loc[index, 'rea_hospDrec_factor'] = nonZeroYbyX(row["hospDRec"],row["rea"])
            
        else :
            dfUrgHosp.loc[index, 'rea_hospDrec_factor'] = statistics.mean(valuesForMean[-10:])
    valuesForMean.append(dfUrgHosp.loc[index, 'rea_hospDrec_factor'])
    previousDep = row['dep']
    print("valuesForMean:", valuesForMean)


###########################################################
# ReCalculate rea (stock) from 24/02 to today reaDRec
###########################################################

dfUrgHosp['reaDRec'] = 0.0
nextReaDRec = 0.0
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep) | (row['day'] > datetime(2020, 3, 17, 0, 0, 0)): #here began with the most recent day
        nextReaDRec = 0.0
        if row['rea_hospDrec_factor']>0 :
            dfUrgHosp.loc[index, 'reaDRec'] = round(row["hospDRec"]*row['rea_hospDrec_factor'],0)
        else : 
            dfUrgHosp.loc[index, 'reaDRec'] = row['rea']
        print("Most recents dates")
    else :  #before 17/03/20
        if row['rea_hospDrec_factor']>0  :
            dfUrgHosp.loc[index, 'reaDRec'] = min(nextdcCumulRec,round(row["hospDRec"]*row['rea_hospDrec_factor'],0))
        else :
            dfUrgHosp.loc[index, 'reaDRec'] = row['rea']
    nextReaDRec = dfUrgHosp.loc[index, 'reaDRec']
    previousDep = row['dep']
    print("nextReaDRec:", nextReaDRec)
    

##########################################################################################
# calculate hospDRecSum and reaDRecSum 
##########################################################################################
# reaCumulRec is calculate using hospCumulRec * reaDRecSum_hospDRecSum_factor
dfUrgHosp.dtypes
#Resort it  day ascending
dfUrgHosp.sort_values(by=['dep', 'day'], ascending=[True, True],  inplace=True)
#Reindex
dfUrgHosp.reset_index(inplace=True, drop=True)  #reset index  

#initialize
previousDep = "00"
dfUrgHosp['hospDRecSum'] = 0.0
dfUrgHosp['reaDRecSum'] = 0.0
previousHospDRecSum = 0.0
previousReaDRecSum  = 0.0

#calculate 
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    print("hospDRec:", row['hospDRec'])
    
    if (index==0) | (row['dep']!=previousDep):
        previousHospDRecSum = 0.0
        previousReaDRecSum  = 0.0
        dfUrgHosp.loc[index, 'hospDRecSum'] = row['hospDRec']
        dfUrgHosp.loc[index, 'reaDRecSum'] = row['reaDRec']
        print("First line")
    else :
        dfUrgHosp.loc[index, 'hospDRecSum'] = row['hospDRec'] + previousHospDRecSum
        dfUrgHosp.loc[index, 'reaDRecSum'] = row['reaDRec'] + previousReaDRecSum 
        print("other lines")
    previousDep = row['dep']
    previousHospDRecSum = dfUrgHosp.loc[index, 'hospDRecSum']
    previousReaDRecSum  = dfUrgHosp.loc[index, 'reaDRecSum']
    print(" previousreaDRecSum:", previousReaDRecSum)


##########################################################################################
# Calculate reaDRecSum_hospDRecSum_factor  using an apply
##########################################################################################

#Resort it  day ascending  to be sure 
dfUrgHosp.sort_values(by=['dep', 'day'], ascending=[True, True],  inplace=True)
#Reindex
dfUrgHosp.reset_index(inplace=True, drop=True)  #reset index  
#calculate using an apply
dfUrgHosp['reaDRecSum_hospDRecSum_factor']  = dfUrgHosp.apply(lambda x: nonZeroYbyX(dfUrgHosp.loc[index, 'hospDRecSum'],dfUrgHosp.loc[index, 'reaDRecSum']), axis=1)  

    
##########################################################################################
# ReaCumulRec
##########################################################################################
# reaCumulRec is calculate using hospCumulRec * reaDRecSum_hospDRecSum_factor
dfUrgHosp.dtypes
#Resort it  day ascending
dfUrgHosp.sort_values(by=['dep', 'day'], ascending=[True, True],  inplace=True)
#Reindex
dfUrgHosp.reset_index(inplace=True, drop=True)  #reset index  

#uinitialize
previousDep = "00"
dfUrgHosp['reaCumulRec'] = 0.0
previousReaCumulRec  = 0.0

#calculate .
for index, row in dfUrgHosp.iterrows():
    print("index:", index)
    print("dep:", row['dep'])
    print("date:", row['day'])
    
    if (index==0) | (row['dep']!=previousDep):
        previousReaCumulRec  = 0.0
        dfUrgHosp.loc[index, 'reaCumulRec'] = round(row["hospCumulRec"]*dfUrgHosp.loc[index, 'reaDRecSum_hospDRecSum_factor'],0)
        print("First line")
    else :
        dfUrgHosp.loc[index, 'reaCumulRec'] = max(previousReaCumulRec , round(row["hospCumulRec"]*dfUrgHosp.loc[index, 'reaDRecSum_hospDRecSum_factor'],0))
        print("other lines")
    previousDep = row['dep']
    previousReaCumulRec  =  dfUrgHosp.loc[index, 'reaCumulRec']
    print(" previousReaCumulRec:", previousReaCumulRec)



###################################################################################################
# calculate newHosp, newRea, newReaOut, newRad, newDc from max0,Current Cumuls - previous Cumuls)
# except for newReaOut = max(0, Previous ReaDRec - current ReaDrec)
###################################################################################################

#initialize columns
dfUrgHosp['newHosp'] = 0.0
dfUrgHosp['newRea'] = 0.0
dfUrgHosp['newReaOut'] = 0.0
dfUrgHosp['newRad'] = 0.0
dfUrgHosp['newDc'] = 0.0

#initaize previous variables: 
previousDep = "00"
previousHospCumulRec = 0.0
previousReaCumulRec  = 0.0
previousReaDRec  = 0.0   #for newReaOut
previousRadCumulRec  = 0.0
previousDcCumulRec  = 0.0

#calculate
for index, row in dfUrgHosp.iterrows():
    #print("index:", index)
    #print("dep:", row['dep'])
    #print("date:", row['day'])

    
    if (index==0) | (row['dep']!=previousDep):
        dfUrgHosp.loc[index, 'newHosp'] = row['hospCumulRec']
        dfUrgHosp.loc[index, 'newRea'] = row['reaCumulRec']
        dfUrgHosp.loc[index, 'newReaOut'] = row['reaDRec']
        dfUrgHosp.loc[index, 'newRad'] = row['radCumulRec']
        dfUrgHosp.loc[index, 'newDc'] = row['dcCumulRec']
        print("First line newDc :", dfUrgHosp.loc[index, 'newDc'])
    else :
        dfUrgHosp.loc[index, 'newHosp'] = max(0,row['hospCumulRec'] - previousHospCumulRec)
        dfUrgHosp.loc[index, 'newRea'] =  max(0,row['reaCumulRec'] - previousReaCumulRec)
        dfUrgHosp.loc[index, 'newReaOut'] = max(0,previousReaDRec - row['reaDRec'])
        dfUrgHosp.loc[index, 'newRad'] = max(0,row['radCumulRec'] - previousRadCumulRec)
        dfUrgHosp.loc[index, 'newDc'] = max(0,row['dcCumulRec'] - previousDcCumulRec)  
        #print("previousDcCumulRec :", previousDcCumulRec)
        print("Other Lines newDc :", dfUrgHosp.loc[index, 'newDc'])
        
    previousHospCumulRec = row['hospCumulRec'] 
    previousReaCumulRec = row['reaCumulRec']
    previousReaDRec = row['reaDRec']
    previousRadCumulRec  = row['radCumulRec']
    previousDcCumulRec  = row['dcCumulRec']
    previousDep = row['dep']





#####################################################################
# Save in Excel to check, if needed
#####################################################################
#Resort it  day ascending
dfUrgHosp.sort_values(by=['dep', 'day'], ascending=[True, True],  inplace=True)
#Reindex
dfUrgHosp.reset_index(inplace=True, drop=True)  #reset index
dfUrgHosp.to_excel("dfUrgHosp.xlsx", sheet_name='UrgHosp', index=True)  


##################################################################################
# II - calculate number of days in Rea
##################################################################################

##############################################################################
#  Read file with newHosp, newRea , newReaOut, newRad, newDc data
##############################################################################


dfNewHosp = pd.read_excel("dfUrgHosp.xlsx", sheet_name='UrgHosp', usecols=["dep", "day", "newHosp", "newRea", "newReaOut", "newDc", "newRad"])

dfNewHosp.dtypes


maxSamples = 100 #number of samples 100 is very good, 20 is enough

column_names = ["numSample", "dep", "dayInReaNZMean", "countDayInReaNZ", "DaysInReaSampleWMean"]
dfAllDepDaysInRea = pd.DataFrame(columns = column_names)

for numSample in range(0,maxSamples) :
        
    print("numSample:", numSample)
    
    #Create a Tidy Data Data Frame :  one row for each hospitalization
    
    column_names = ["dep", "dayHosp", "dayRea", "dayReaOut", "dayDc", "dayRad", "closed"]
    dfTidyHosp = pd.DataFrame(columns = column_names)
    #dfTidyHosp.dtypes
    #Normally sorted but force it
    dfNewHosp.sort_values(['dep', 'day'], ascending=[True, True],  inplace=True)
    
    #dfNewHosp.dtypes
    #First Create the Tidy rows from new hospitalizations
    #Add new rows for new days
    for index, row in dfNewHosp.iterrows():
        #print("index:",index)
        #add new hospitalizations
        if row['newHosp']>0:
            #print("dep", row["dep"])
            #print("jour", row["day"])
            dfOneTidyHosp = pd.DataFrame({"dep" : [row["dep"]],
                                             "dayHosp" : [row["day"]], 
                                             "dayRea" : datetime(1970, 1, 1, 0, 0, 0),
                                             "dayReaOut" : datetime(1970, 1, 1, 0, 0, 0),
                                             "dayDc" : datetime(1970, 1, 1, 0, 0, 0),
                                             "dayRad" : datetime(1970, 1, 1, 0, 0, 0),
                                             "closed": False}) #New work Tidy
            
            #☺Splits Rows
            if row['newHosp']>1:    
                dfOneTidyHosp = dfOneTidyHosp.loc[dfOneTidyHosp.index.repeat(row['newHosp'])] 
                dfOneTidyHosp.reset_index(inplace=True, drop=True)  #reset index
            dfTidyHosp = pd.concat([dfTidyHosp,dfOneTidyHosp])  #new rows
            #reset index for global Tidy hosp dataframe
            dfTidyHosp.reset_index(inplace=True, drop=True)  #reset index
            
    
            #############################################################
            #Add newRea
            if row['newRea']>0:   
                #select not already in Rea
                for i in range(0,int(row['newRea'])) :
                    #print("i rea:",i)
                    #select not in rea 
                    dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                         (dfTidyHosp["dayRea"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayReaOut"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["closed"] == False)  ]
                    
                    
                    if dfTidyIndex.shape[0]>0 :
                        myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                        print("dfTidyIndex0Rea:",dfTidyIndex.index[myIndex])
                        dfTidyHosp["dayRea"].iloc[dfTidyIndex.index[ myIndex]]= row["day"]
                        break
    
    
                        
            #####################################            
            #Add newReaOut
            if row['newReaOut']>0:  
                #print("lits de suite:",row['newReaOut'])
                #print("dep", row["dep"])
                #print("jour", row["day"])
                #select only in Rea
                for i in range(0,int(row['newReaOut'])) :
                    #print("i lits de suite:",i)
                    #select only in rea 
                    dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                         (dfTidyHosp["dayRea"] != datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayReaOut"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["closed"] == False)  ]
    
                    if dfTidyIndex.shape[0]>0 :
                        myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                        print("dfTidyIndex0ReaOut:",dfTidyIndex.index[myIndex])
                        dfTidyHosp["dayReaOut"].iloc[dfTidyIndex.index[myIndex]]= row["day"]
                        break
    
    
            #Add newRad  two choices
            if row['newRad']>0:   
                 #select first no Rea  !!! Proba is more certain
                 for i in range(0,int(row['newRad'])) :
                     #print("i rad:",i)
                     #select first no rea
                     dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                          (dfTidyHosp["dayRea"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                          (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                          (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                          (dfTidyHosp["closed"] == False)  ]
                     
                     
                     if dfTidyIndex.shape[0]>0 :
                         myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                         #print("myIndex:", myIndex)
                         print("dfTidyIndex0RadNoRea:",dfTidyIndex.index[myIndex])
                         dfTidyHosp["dayRad"].iloc[dfTidyIndex.index[myIndex]]= row["day"]
                         dfTidyHosp["closed"].iloc[dfTidyIndex.index[myIndex]]= True
                         break
    
    
                 #next select Rea or not  but <=  row["day"]
                
                 for j in range(i,int(row['newRad'])) :
                    #print("i rad rea or not:",i)
                    #select Rea <=  row["day"]
                    dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                         (dfTidyHosp["dayRea"] <=  row["day"] ) &
                                                         (dfTidyHosp["dayReaOut"] <=  row["day"] ) &
                                                         (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["closed"] == False)  ]
                    
                    
                    if dfTidyIndex.shape[0]>0 :
                        myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                        #print("myIndex:", myIndex)
                        print("dfTidyIndex0RadRea:",dfTidyIndex.index[myIndex])
                        dfTidyHosp["dayRad"].iloc[dfTidyIndex.index[myIndex]]= row["day"]
                        dfTidyHosp["closed"].iloc[dfTidyIndex.index[myIndex]]= True
                        break
    
                   
           #Add newDc
            if row['newDc']>0:   
                #select first  Rea only  #proba is up to die
                for i in range(0,int(row['newDc'])) :
                    #print("i Dc:",i)
                    #select first Rea
                    dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                         (dfTidyHosp["dayRea"] > datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["closed"] == False)  ]
                    
                    
                    if dfTidyIndex.shape[0]>0 :
                        myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                        print("dfTidyIndex0DcRea:",dfTidyIndex.index[myIndex])
                        dfTidyHosp["dayDc"].iloc[dfTidyIndex.index[myIndex]]= row["day"]
                        dfTidyHosp["closed"].iloc[dfTidyIndex.index[myIndex]]= True
                        break
    
                #next select Rea or not
                
                for j in range(i,int(row['newDc'])) :
                    #print("i rea:",i)
                    #select not Rea or Not
                    dfTidyIndex =  dfTidyHosp.loc [ (dfTidyHosp["dep"] == row["dep"]) & 
                                                         (dfTidyHosp["dayDc"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["dayRad"] == datetime(1970, 1, 1, 0, 0, 0)) & 
                                                         (dfTidyHosp["closed"] == False)  ]
                    
                    
                    if dfTidyIndex.shape[0]>0 :
                        myIndex = random.randint(0,dfTidyIndex.shape[0]-1)
                        print("dfTidyIndex0DcAll:",dfTidyIndex.index[myIndex])
                        dfTidyHosp["dayDc"].iloc[dfTidyIndex.index[myIndex]]= row["day"]
                        dfTidyHosp["closed"].iloc[dfTidyIndex.index[myIndex]]= True
                        break
    
    
    #dfTidyHosp.dtypes   
      
    #Calculate Days in Rea for this sample

              
    def calculateDaysInRea(x,y,z,t):
        #x = "dayRea"
        #y = "dayReaOut"
        #z = "dayDc" 
        #t = "dayRad"
        #if rea 
        if x>datetime(1970, 1, 1, 0, 0, 0) :
            #Lits de Suite ????
            if y>datetime(1970, 1, 1, 0, 0, 0) :
                #Lits de Suite OK
                minOut=y
            else :
                #Dead ???
                if z>datetime(1970, 1, 1, 0, 0, 0) :
                     minOut=z
                else :
                    #Rad/out ????
                    if t>datetime(1970, 1, 1, 0, 0, 0) :
                        minOut=z
                    else :#still in hosp -> out is today for the mment
                        minOut= datetime.today()
            if  minOut >  datetime(1970, 1, 1, 0, 0, 0) : #not sure it is necessary
                    myDelta = minOut-x
                    print("days:",myDelta.days+1 )
                    return  myDelta.days+1        
        else : return None
    
            
    dfTidyHosp["daysInRea"]  = dfTidyHosp.apply(lambda x: calculateDaysInRea(x["dayRea"], x["dayReaOut"], x["dayDc"], x["dayRad"] ), axis=1)           
    
   
    strNumSample = str(numSample)
    dfTidyHosp.reset_index(inplace=True, drop=True) #reset index
    #Save the Sample - long to save could be in comments
    print("Save the Sample Tidy")
    #dfTidyHosp.to_excel("TidyHosp"+str(numSample)+".xlsx", sheet_name='Tidy', index=True)    
    ######## 
    
    
    #################################################################################
    # Calculate number of days in rea by department for this sample
    ###### create meanRea DF (by Department)
    uniqueDep = dfTidyHosp["dep"].unique() #
    
    column_names = ["numSample", "dep", "dayInReaNZMean", "countDayInReaNZ", "DaysInReaSampleWMean"]
    dfDepDaysInRea = pd.DataFrame(columns = column_names)
    
    for dep in uniqueDep :
        print("dep:",dep)
        dfTidyHospDep =  dfTidyHosp.loc [ dfTidyHosp["dep"] == dep ]
        dayInReaNZMean = dfTidyHospDep["daysInRea"].mean( skipna = True)
        countDayInReaNZ = dfTidyHospDep["daysInRea"].dropna().size  #weight 
        print("dayInReaNZMean:",dayInReaNZMean)
        print("countDayInReaNZ:",countDayInReaNZ)
        dfOneDepDaysInRea = pd.DataFrame({"numSample": numSample, "dep" : dep, "dayInReaNZMean" : dayInReaNZMean, "countDayInReaNZ" : countDayInReaNZ}, index=[0]) #New work Tidy
        
        dfDepDaysInRea = pd.concat([dfDepDaysInRea,dfOneDepDaysInRea])  #new rows
    
    dfDepDaysInRea["DaysInReaSampleWMean"] = np.average(a=dfDepDaysInRea["dayInReaNZMean"],weights=dfDepDaysInRea["countDayInReaNZ"] )  

    
    dfDepDaysInRea.reset_index(inplace=True, drop=True) 
    print("Save one Sample Days in Rea by Day")
    #Save one sample file if needed
    #dfDepDaysInRea.to_excel("dfDepDaysInRea"+str(numSample)+".xlsx", sheet_name='Means', index=True)    
    #Concat data in all samples data 
    dfAllDepDaysInRea = pd.concat([dfAllDepDaysInRea,dfDepDaysInRea]) #new rows
    

print("Save all Samples Days in Rea by Day")
#Calculate Global average with weignt
dfAllDepDaysInRea["DaysInReaGlobalWMean"] = np.average(a=dfAllDepDaysInRea["dayInReaNZMean"],weights=dfAllDepDaysInRea["countDayInReaNZ"] )
dfAllDepDaysInRea.reset_index(inplace=True, drop=True) 
dfAllDepDaysInRea.to_excel("dfAllDepDaysInRea.xlsx", sheet_name='Means', index=True)                 
                
####################################################################################################
# FINAL FILE
# Global mean of samples by dep
####################################################################################################
#Resort by dep
dfAllDepDaysInRea.dtypes

dfAllDepDaysInRea['dep']= dfAllDepDaysInRea['dep'].astype(str)  #force dep to string
dfAllDepDaysInRea['dep'] = dfAllDepDaysInRea['dep'].str.strip() #strip white spaces
dfAllDepDaysInRea.sort_values(by='dep', ascending=True,  inplace=True)
#reindex
dfAllDepDaysInRea.reset_index(inplace=True, drop=True) 


#createnew df for days in Rea by dep for all samples
column_names =  ["dep", "daysInReaByDep"]
dfDaysInReaByDep = pd.DataFrame(columns = column_names)                     
uniqueDep = dfAllDepDaysInRea["dep"].unique() #

for dep in uniqueDep :
    print("dep:",dep)
    #create one dataframe for one dep
    daysInReaByDep = 0.0  #initialize value
    #dfOneDaysInReaByDep = pd.DataFrame(columns = column_names) 
    #select data for the current dep
    dfOneAllDepDaysInRea =  dfAllDepDaysInRea.loc [ dfAllDepDaysInRea["dep"] == dep ]
    
    #calculate average for this dep
    daysInReaByDep = np.average(a=dfOneAllDepDaysInRea["dayInReaNZMean"],weights=dfOneAllDepDaysInRea["countDayInReaNZ"] )
    print("average:",dfOneDaysInReaByDep["daysInReaByDep"] )
    
    dfOneDaysInReaByDep = pd.DataFrame({"dep": dep,  "daysInReaByDep" : daysInReaByDep}, index=[0]) #New work df
    #concat in all dep dataframe        
    dfDaysInReaByDep = pd.concat([dfDaysInReaByDep,dfOneDaysInReaByDep])  #new rows
                         
dfDaysInReaByDep.reset_index(inplace=True, drop=True) 
dfDaysInReaByDep.to_excel("dfDaysInReaByDep.xlsx", sheet_name='Days', index=False)                      
                     
                     