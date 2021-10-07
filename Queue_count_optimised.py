import sqlite3
import uuid
import pandas as pd


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


def main_logic():
    
     
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
    
     
    for index, row in df.iterrows():
        merchant = ['Merchant1', 'Merchant2', 'Merchant3', 'Merchant4', 'Merchant5']
        env = ['Env1', 'Env2', 'Env3', 'Env4', 'Env5']
        dict = {}
        id_val = str(row['id1'])
        for i in range(len(merchant)):
            if (row[merchant[i]]!= '0'):
                
                if (dict.get(row[merchant[i]]) == None):
                    dict[row[merchant[i]]] = str(uuid.uuid4())        
                cursor = sqlite_conn.cursor()
                print(row[merchant[i]])
                print(row[env[i]])
                st = 'INSERT INTO Queue_' + str(row[env[i]]) + ' (id, indirectId) VALUES' + '(' + id_val +',' + "'" + dict[row[merchant[i]]] + "'" + ')'
                #st2 = 'UPDATE Queue_' + env + ' SET indirectId=' + str(uuid.uuid4()) + ' where id =' + id_val
                print(st)
                cursor.execute(st)
 
    
 
    
main_logic()    