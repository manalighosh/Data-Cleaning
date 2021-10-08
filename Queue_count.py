# -*- coding: utf-8 -*-

"""
Created on Sun Sep 15 18:39:54 2019

@author: Manali
"""
import sqlite3
import uuid
import pandas as pd
#import os 
#import mysql.connector

#:\Users\Manali\Desktop\Manali_T\unique_queue.xlsx

#reading the sheet of a workbook
df = pd.read_excel('C:\\Users\\ghosh\\OneDrive\\Desktop\\Excel_queue\\manaliq.xlsx',sheet_name=2,index_col=None)

#df['a'] = [x.replace(',', '-') for x in df['a']]----->>>>>'a' is the column name
df['Merchant1'] = df['Merchant1'].astype(str)
df['Merchant1'] = [x.replace(',', '-') for x in df['Merchant1']]

df['Merchant2'] = df['Merchant2'].astype(str)
df['Merchant2'] = [x.replace(',', '-') for x in df['Merchant2']]

df['Merchant3'] = df['Merchant3'].astype(str)
df['Merchant3'] = [x.replace(',', '-') for x in df['Merchant3']]

df['Merchant4'] = df['Merchant4'].astype(str)
df['Merchant4'] = [x.replace(',', '-') for x in df['Merchant4']]

df['Merchant5'] = df['Merchant5'].astype(str)
df['Merchant5'] = [x.replace(',', '-') for x in df['Merchant5']]


#df['Merchant1'] = df['Merchant1'].astype(float)
  

env = ""
e2 = ""  
e3 = ""
e4 = ""
e5 = ""

def main_logic():
    
    env = ""
    e2 = ""  
    e3 = ""
    e4 = ""
    e5 = ""

    
    sqlite_conn = sqlite3.connect(':memory:')
    #sqlite_conn = sqlite3.connect(‘:memory:’)
    cursor = sqlite_conn.cursor()
    print("database created")
    
    create_table_us = """
    CREATE TABLE IF NOT EXISTS `Queue_US` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_us)
    print("us created")
    
    create_table_us1 = """
    CREATE TABLE IF NOT EXISTS `Queue_US1` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_us1)
    print("us1 created")
    
    
    create_table_us2 = """
    CREATE TABLE IF NOT EXISTS `Queue_US2` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_us2)
    print("us2 created")
    
    
    create_table_us3 = """
    CREATE TABLE IF NOT EXISTS `Queue_US3` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_us3)
    print("us3 created")
    
    
    create_table_na1 = """
    CREATE TABLE IF NOT EXISTS `Queue_NA1` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_na1)
    print("na1 created")
    
    
    create_table_na2 = """
    CREATE TABLE IF NOT EXISTS `Queue_NA2` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_na2)
    print("na2 created")
    
    
    create_table_na3 = """
    CREATE TABLE IF NOT EXISTS `Queue_NA3` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_na3)
    print("na3 created")
    
    
    create_table_na4 = """
    CREATE TABLE IF NOT EXISTS `Queue_NA4` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_na4)
    print("na4 created")
    
    
    create_table_ca1 = """
    CREATE TABLE IF NOT EXISTS `Queue_CA1` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_ca1)
    print("ca1 created")
    
    
    create_table_au1 = """
    CREATE TABLE IF NOT EXISTS `Queue_AU1` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_au1)
    print("au1 created")
    
    create_table_eur = """
    CREATE TABLE IF NOT EXISTS `Queue_EUR` (`id` INTEGER PRIMARY KEY, `indirectId` TEXT DEFAULT NULL);
    """
    cursor.execute(create_table_eur)
    print("eur created")
    
    
    i = 0
    ids_appearing_once = 0
    ids_appearing_twice = 0 
    ids_appearing_thrice = 0 
    ids_appearing_4X = 0 
    ids_appearing_5X = 0 
    else_ids = 0


    for index, row in df.iterrows():
        #i = i + row['C1']
        #print (row['Merchant1'])
        #All 5 Merchants NOT EQUAL TO 0
        if (row['Merchant1']!= '0' and row['Merchant2']!= '0' and row['Merchant3']!= '0' and row['Merchant4']!= '0' and row['Merchant5']!='0' ):
            
            #print(id_val)
            #All 5 Merchants are different 
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e5 = (row['Env5'])    
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
               
            #if two Merchants are same************************************************
            #M1 and M2 are same **************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp = uuid.uuid4()
                #print (pp)
                
                id_val= str(row['id1'])
                #print (id_val)
                    
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(pp) + ' where id =' + id_val
                query = cursor.execute(st)    
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(pp) + ' where id =' + id_val
                query = cursor.execute(st)  
                
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                    
                e5 = (row['Env5'])    
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1 and M3 are same **********************************************************   
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp1 = uuid.uuid4()
                #print (pp1)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(pp1) + ' where id =' + id_val
                query = cursor.execute(st)     
                    
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)    
                
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(pp1) + ' where id =' + id_val
                query = cursor.execute(st)  
                
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1 and M4 are same *********************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp2 = uuid.uuid4()
                #print (pp2)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(pp2) + ' where id =' + id_val
                query = cursor.execute(st) 
                    
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4+ ' SET indirectId=' + str(pp2) + ' where id =' + id_val
                query = cursor.execute(st) 
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
            
            #M1 and M5 are same ******************************************************    
            if(row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp3 = uuid.uuid4()
                #print (pp3)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(pp3) + ' where id =' + id_val
                query = cursor.execute(st)  
    
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(pp3) + ' where id =' + id_val
                query = cursor.execute(st)
            
            #M2 and M3 are same *****************************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp4 = uuid.uuid4()
                #print (pp4)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(pp4) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(pp4) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2 and M4 are same *************************************************   
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp5 = uuid.uuid4()
                #print (pp5)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(pp5) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(pp5) + ' where id =' + id_val
                query = cursor.execute(st) 
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2 and M5 are same****************************************************   
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp6 = uuid.uuid4()
                #print (pp6)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) + "'"+')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)             
                          
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp6) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(pp6) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp6) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(pp6) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M3 and M4 are same****************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp7 = uuid.uuid4()
                #print (pp7)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp7) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(pp7) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp7) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(pp7) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M3 and M5 are same ***************************************   
            if(row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                pp8 = uuid.uuid4()
                #print (pp8)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp8) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(pp8) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp8) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(pp8) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M4 and M5 are same********************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                pp9 = uuid.uuid4()
                #print (pp9)
                id_val= str(row['id1'])
                print(id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = str(row['Env4'])   
                print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp9) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(pp9) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(pp9) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(pp9) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #Three Merchants are same
            #M1,M2,M3 are same ******************************************
            if(row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg = str(uuid.uuid4())
                #print (gg)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2, M3 , M4 are same ******************************************************
            if(row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg1 = str(uuid.uuid4())
                #print (gg1)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)                  
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
             #M3,M4, M5 are same **************************************************
            if(row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                gg2 = str(uuid.uuid4())
                #print (gg2)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M4, M5, M1 are same ******************************************
            if(row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                gg3 = str(uuid.uuid4())
                #print (gg3)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3+ ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M5, M1, M2 are same ***********************************************
            if(row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg4 = str(uuid.uuid4())
                #print (gg4)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3+ ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1, M2, M4 are same *********************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg5 = str(uuid.uuid4())
                #print (gg5)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg5) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2, M3, M5 are same ***********************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg6 = str(uuid.uuid4())
                #print (gg6)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg6) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg6) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg6) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg6) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg6) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg6) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1, M3, M4 are same ***************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg7 = str(uuid.uuid4())
                #print (gg7)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg7) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg7) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg7) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg7) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg7) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg7) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
             #M2, M4, M5  are same***********************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                gg8 = str(uuid.uuid4())
                #print (gg8)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)    
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg8) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg8) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg8) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg8) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg8) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg8) + ' where id =' + id_val
                query = cursor.execute(st)
                
             #M1, M3, M5 are same *******************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                gg9 = str(uuid.uuid4())
                #print (gg9)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg9) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg9) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg9) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg9) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg9) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(gg9) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #four Merchants are same**************************************************************
            #M1, M2, M3, M4 are same**************************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                cc = uuid.uuid4()
                #print (cc)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(cc) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(cc) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(cc) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(cc) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])   
                #print (e5)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 
                query = cursor.execute(st)
                
            #M2, M3, M4, M5 are same*************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] != row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                cc1 = uuid.uuid4()
                #print (cc1)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])   
                #print (env)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(cc1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(cc1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(cc1) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(cc1) + ' where id =' + id_val
                query = cursor.execute(st)
                
             #M3, M4, M5, M1 are same*****************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] != row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                cc2 = uuid.uuid4()
                #print (cc2)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(cc2) + ' where id =' + id_val
                query = cursor.execute(st)    
    
                e2 = (row['Env2'])   
                #print (e2)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(cc2) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(cc2) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(cc2) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #M1, M2, M4,  M5 are same *****************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3']!= row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                cc3 = uuid.uuid4()
                #print (cc3)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(cc3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(cc3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])   
                #print (e3)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(cc3) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(cc3) + ' where id =' + id_val
                query = cursor.execute(st)  
                
            #M1, M2, M3, M5 are same**************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] != row['Merchant4'] and row['Merchant3'] != row['Merchant5'] and row['Merchant4'] != row['Merchant5']):
                cc4 = uuid.uuid4()
                #print (cc4)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(cc4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(cc4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(cc4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(cc4) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #if 5 Merchants are same*************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant1'] == row['Merchant5'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant2'] == row['Merchant5'] and row['Merchant3'] == row['Merchant4'] and row['Merchant3'] == row['Merchant5'] and row['Merchant4'] == row['Merchant5']):
                cc5 = uuid.uuid4()
                #print (cc5)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(cc5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(cc5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(cc5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(cc5) + ' where id =' + id_val
                query = cursor.execute(st)
                    
                e5 = (row['Env5'])    
                #print(e5)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e5 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(cc5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e5 + ' SET indirectId=' + str(cc5) + ' where id =' + id_val
                query = cursor.execute(st) 
            i += 5        

            ids_appearing_5X += 1


        # When one Merchant IS EQUAL TO 0:  M5 = 0       
        if (row['Merchant5'] == '0' and row['Merchant1'] != '0' and row['Merchant2'] != '0' and row['Merchant3'] != '0' and row['Merchant4'] != '0'):
            
            #if all 4 Merchants are different*******************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #if 2 Merchants are same****************************************
            # M1 and M2 are same******************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                aa = uuid.uuid4()
                #print (aa)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(aa) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(aa) + ' where id =' + id_val
                query = cursor.execute(st)    
                
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1 and M3 are same*****************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                aa1 = uuid.uuid4()
                #print (aa1)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(aa1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(aa1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1 and M4 are same*********************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                aa2 = uuid.uuid4()
                #print (aa2)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(aa2) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])  
                #print(e3)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(aa2) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2 and M3 are same*************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                aa3 = uuid.uuid4()
                #print (aa3)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(aa3) + ' where id =' + id_val
                query = cursor.execute(st)    
                #print (st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(aa3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #M2 and M4 are same******************************************* 
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                aa4 = uuid.uuid4()
                #print (aa4)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(aa4) + ' where id =' + id_val
                query = cursor.execute(st)   
                #print (st)
                
                e3 = (row['Env3'])  
                #print(e3)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(aa4) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #M3 and M4 are same *********************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] == row['Merchant4']):
                aa5 = uuid.uuid4()
                #print (aa5)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(aa5) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(aa5) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(aa5) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #3 Merchants are same
            #M1, M2, M3 same**********************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                gg = uuid.uuid4()
                #print (gg)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)   
                #print (st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])   
                #print (e4)
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2,M3, M4 are same *******************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] != row['Merchant4'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant3'] == row['Merchant4']):
                gg1 = uuid.uuid4()
                #print (gg1)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st)   
                
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg1) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #M3, M4, M1 are same**************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] != row['Merchant4'] and row['Merchant3'] == row['Merchant4']):
                gg2 = uuid.uuid4()
                #print (gg2)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)
                
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg2) + ' where id =' + id_val
                query = cursor.execute(st)  
                
            #M1, M2, M4 are same********************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant2'] != row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant3'] != row['Merchant4']):
                gg3 = uuid.uuid4()
                #print (gg3)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])  
                #print(e3)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg3) + ' where id =' + id_val
                query = cursor.execute(st) 
                
            #if all 4 are same************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant1'] == row['Merchant4'] and row['Merchant2'] == row['Merchant3'] and row['Merchant2'] == row['Merchant4'] and row['Merchant3'] == row['Merchant4']):
                gg4 = uuid.uuid4()
                #print (gg4)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e4 = (row['Env4'])    
                #print(e4)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e4 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(gg4) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e4 + ' SET indirectId=' + str(gg4) + ' where id =' + id_val
                query = cursor.execute(st)
            i += 4
            ids_appearing_4X += 1
        
        # Two Merchants are zero: M4=0, M5=0********************     
        if (row['Merchant1']!= '0' and row['Merchant2']!= '0' and row['Merchant3']!= '0' and row['Merchant4'] == '0' and row['Merchant5'] == '0' ):
            
            #if all three M1, M2, M3 are different********************************
            if(row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant2'] != row['Merchant3'] ):
                
                id_val= str(row['id1'])
                #print (id_val)                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'" + str(uuid.uuid4()) +"'" + ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                #print(st)
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'" + str(uuid.uuid4()) +"'" + ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #if M1, M2 are same*************************************************
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant2'] != row['Merchant3'] ):
                
                ff = uuid.uuid4()
                #print(ff)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(ff) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(ff) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])    
                #print (e3)     
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #if M1, M3 are same*****************************************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant2'] != row['Merchant3'] ):
                
                ff1 = uuid.uuid4()
                #print(ff1)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(ff1) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff1) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(ff1) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M2, M3 are same**********************************
            if (row['Merchant1'] != row['Merchant2'] and row['Merchant1'] != row['Merchant3'] and row['Merchant2'] == row['Merchant3'] ):
                
                ff2 = str(uuid.uuid4())
                #print(ff2)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(uuid.uuid4()) + "'"+')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff2) + "'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(ff2) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'"+ str(ff2) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(ff2) + ' where id =' + id_val
                query = cursor.execute(st)
                
            #M1, M2, M3 all are same
            if (row['Merchant1'] == row['Merchant2'] and row['Merchant1'] == row['Merchant3'] and row['Merchant2'] == row['Merchant3'] ):
                
                ff3 = uuid.uuid4()
                #print(ff3)
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(ff3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(ff3) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(ff3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(ff3) + ' where id =' + id_val
                query = cursor.execute(st) 
                
                e3 = (row['Env3'])    
                #print(e3)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e3 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(ff3) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e3 + ' SET indirectId=' + str(ff3) + ' where id =' + id_val
                query = cursor.execute(st)
            i += 3
            ids_appearing_thrice += 1
        
        #M1, M2 NOT EQUAL TO 0
        if (row['Merchant1']!= '0' and row['Merchant2']!= '0' and row['Merchant3']== '0' and row['Merchant4']== '0' and row['Merchant5']=='0' ): 
            
            #M1, M2 are different*******************************************
            
            if (row['Merchant1'] != row['Merchant2'] ):
                
                id_val= str(row['id1'])
                #print(id_val)
                
                env = (row['Env1'])    
                #print(type(env))       
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])  
                e2 = str(e2)
                #print(e2)    
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                query = cursor.execute(st)
                
                
            #M1, M2 are same*******************************************
            if (row['Merchant1'] == row['Merchant2'] ):
                hh = uuid.uuid4()
                #print (hh)
                
                id_val= str(row['id1'])
                #print (id_val)
                
                env = (row['Env1'])    
                #print(env)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(hh) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(hh) + ' where id =' + id_val
                query = cursor.execute(st)
                
                e2 = (row['Env2'])    
                #print(e2)        
                cursor = sqlite_conn.cursor()
                st = 'INSERT INTO Queue_' + e2 + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(hh) +"'"+ ')'
                #st2 = 'UPDATE Queue_' + e2 + ' SET indirectId=' + str(hh) + ' where id =' + id_val
                query = cursor.execute(st) 
            i += 2                
            ids_appearing_twice += 1
        
        #Only M1 NOT EQUAL TO 0        
        if (row['Merchant1']!= '0' and row['Merchant2']== '0' and row['Merchant3']== '0' and row['Merchant4']== '0' and row['Merchant5']=='0'):
            id_val= str(row['id1'])
            #print (id_val)
            
            env = (row['Env1'])    
            #print(env)        
            cursor = sqlite_conn.cursor()
            st = 'INSERT INTO Queue_' + env + ' (id, indirectId) VALUES' + '(' + id_val +',' +"'"+ str(uuid.uuid4()) +"'"+ ')'
            #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
            query = cursor.execute(st)
            i += 1 
            ids_appearing_once += 1
        #print("total rows visited")
        else: 
            else_ids += 0
#        
        #print(index)
    print(i)
    print("ROWS of IDS 1 {}".format(ids_appearing_once))
    print("ROWS of IDS 2 {}".format(ids_appearing_twice))
    print("ROWS of IDS 3 {}".format(ids_appearing_thrice))
    print("ROWS of IDS 4 {}".format(ids_appearing_4X))
    print("ROWS of IDS 5 {}".format(ids_appearing_5X)) 
    print("ROWS not captured by conditions: {}".format(else_ids))    
#        print(i)    
 
 #print any tables' count   
    total_records_inserted = 0
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_US")
    results = cursor.fetchall()
    print("US {}".format(len(results)))
    total_records_inserted += len(results)
    
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_US1")
    results = cursor.fetchall()
    print("US1 {}".format(len(results)))
    total_records_inserted += len(results)

    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_US2")
    results = cursor.fetchall()
    print("US2 {}".format(len(results)))
    total_records_inserted += len(results)
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_US3")
    results = cursor.fetchall()
    print("US3 {}".format(len(results)))
    total_records_inserted += len(results)
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_NA1")
    results = cursor.fetchall()
    print("NA1 {}".format(len(results)))
    total_records_inserted += len(results)
    
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_NA2")
    results = cursor.fetchall()
    print("NA2 {}".format(len(results)))
    total_records_inserted += len(results)
    
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_NA3")
    results = cursor.fetchall()
    print("NA3 {}".format(len(results)))
    total_records_inserted += len(results)

    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_NA4")
    results = cursor.fetchall()
    print("NA4 {}".format(len(results)))
    total_records_inserted += len(results)

    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_AU1")
    results = cursor.fetchall()
    print("AU1 {}".format(len(results)))
    total_records_inserted += len(results)
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_CA1")
    results = cursor.fetchall()
    print("CA1 {}".format(len(results)))
    total_records_inserted += len(results)
    
    cursor = sqlite_conn.cursor()    
    cursor.execute("select * from Queue_EUR")
    results = cursor.fetchall()
    
    print("EUR {}".format(len(results)))
    total_records_inserted += len(results)
    
    print("Total Records Inserted " + str(total_records_inserted))
       

main_logic()            
      
          
            
        
            
            
            
            
        
        
            
        
            
         
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        



