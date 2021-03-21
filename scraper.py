#pylint:disable=W0311
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
from datetime import timedelta
import base64
import shutil
import os.path
from apikey import *
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId)
import schedule
import time
import pytz
import re
import csv
from csv import DictWriter
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2

gChromeOptions = webdriver.ChromeOptions()
gChromeOptions.add_argument("window-size=1920x1480")
gChromeOptions.add_argument("disable-dev-shm-usage")
gDriver = webdriver.Chrome(
chrome_options=gChromeOptions, executable_path=ChromeDriverManager().install()
)

def insert():
    try:
        directory= os.path.dirname(os.path.realpath(__file__))
        filename = "virtual.csv"
        file_path3 = os.path.join(directory,'clean/', filename)
        DATABASE_URL = os.environ['DATABASE_URL']
        connection = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = connection.cursor()
        
        with open(file_path3, 'r') as f:
            
            # Notice that we don't need the `csv` module.
            next(f) # Skip the header row.
            cursor.copy_from(f, 'virtuals', sep=',')
            
       
        connection.commit()
        print('successfully added data')
    
    
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    



def data():
        url = 'https://www.fortebet.ug/#/app/virtualsoccer/results'
        gDriver.get(url)
        time.sleep (3)
        page = gDriver.page_source
        soup = BeautifulSoup(page, 'lxml')          
        print('connected to web')
        directory= os.path.dirname(os.path.realpath(__file__))
        filename = "virtuals.html"
        file_path = os.path.join(directory,'clean/', filename)     
        soup1 = soup.prettify()       
        with open(file_path, 'w') as f:
            f.writelines(str(soup1))
        print('created virtuals file')

def tim():
    with open('time.txt', 'w') as f:
       x = datetime.datetime.now()      
       b = x + timedelta(minutes=15) 
       w = b.strftime("%b %d, %Y %H:%M") 
       f.write(str(w))       

def final_s():
            directory= os.path.dirname(os.path.realpath(__file__))
            filename2 = "results.csv"
            final_r = "virtual.csv"
            filename = "scrapedfile.csv"
            file_path = os.path.join(directory,'clean/', filename)
            file_path3 = os.path.join(directory,'clean/', final_r)
            file_path2 = os.path.join(directory,'clean/', filename2)
            f = pd.read_csv(file_path)    
            h = pd.read_csv(file_path2)            
            df_merge_col = pd.merge(f, h, on='Match No')
            del df_merge_col['HomeTeam_y']
            del df_merge_col['AwayTeam_y']
            
          
            df_merge_col = df_merge_col.rename(columns={'HomeTeam_x': 'HomeTeam','AwayTeam_x': 'AwayTeam' })
            
            df_merge_col.to_csv(file_path3, index=False)
             
            file_data = pd.read_csv(file_path3)
            dr = file_data.drop_duplicates(subset=['Match No'], keep='first')
            dr.to_csv(file_path3, index=False) 
            insert()
            with open(file_path3, 'rb') as file:
                data_ = file.read()
                file.close()
        
            encoded = base64.b64encode(data_).decode()
            message = Mail(
            from_email=FROM_EMAIL,
            to_emails=TO_EMAIL,
            subject='Your File is Ready',
            html_content='<strong>Attached is Your Scraped File</strong>')
            attachment = Attachment()
            attachment.file_content = FileContent(encoded)
            attachment.file_type = FileType('text/csv')
            x = datetime.datetime.now() 
            w = x.strftime("%d_%b_%Y")
            filename = 'virtuals_'+str(w)+'.csv'                
            attachment.file_name = FileName(filename)
            attachment.disposition = Disposition('attachment')
            attachment.content_id = ContentId('Example Content ID')
            message.attachment = attachment
            try:
                sg = SendGridAPIClient(SENDGRID_API_KEY)
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)
            except Exception as e:
                 print(e)
            os.remove(file_path)
            print('combined the two csv files and created the final virtuals dataframe\nAnd then deleted the scrapedfile')


def extract():
       data()   
       directory= os.path.dirname(os.path.realpath(__file__))
       filename2 = "results.csv"
       filename = "virtuals.html"
       file_path = os.path.join(directory,'clean/', filename)
       file_path2 = os.path.join(directory,'clean/', filename2)
       with open(file_path)  as f:
            g = f.readlines()
            pattern1 = '[V][-|.]\S+\W[-]\W[V][-|.]\S+'  #pattern for teams
            pattern2 = '\d[:]\d\W[(]\d[:]\d[)]'   #pattern for scores
            pattern3 = '\D\D\D\W\d\d[,]\W\d\d\d\d\W\d\d[:]\d\d'   #pattern for date
            pattern4 = "    (\d{1,3})[^:]"  #pattern for match number
            dates = re.findall(pattern3, str(g))
            
            teams = re.findall(pattern1,str(g))
            scores = re.findall(pattern2, str(g))
            mat_nos = re.findall(pattern4, str(g))
            
            
            mat_l = []
            sc_l = []
            tea_l =[]
            records = []
            
            for mat_no in mat_nos[1:-5]:
                mat_no = f'{mat_no}'
                mat_l.append(mat_no)  
            for score in scores[:-3]:
                pattern = '\D'
                s = re.split(pattern, score)
                h_f= s[0]#home full time score
                a_f = s[1]#away full time score
                h_h = s[3]#home half time score
                a_h = s[4]#away half time score
                score = f'\'{h_f}:{a_f} ({h_h}:{a_h})\''               
                sc_l.append(score)
            for team in teams[:-3]:
                tea = team[:-4]
                pattern = '[-]'
                te = re.split(pattern, tea)
                if len(te) == 4:
                    home = te[1]   #home team
                    away = te[-1]       #away team
                    t = f'\'{home} - {away}\''
                    tea_l.append(t)
                 
                else:
                    home = te[0]   #home team
                    away = te[-1]       #away team
                    t = f'\'{home} - {away}\''
                    tea_l.append(t)
            
            for (a, b, c) in zip(mat_l, tea_l, sc_l):
               record = f'\'{a}\', {b}, {c}'
               
               records.append(record)
            res = list(map(eval, records)) 
          
            print('Successfully extracted data')
            print('creating results.csv file\n')
            rest = list(reversed(res))
            data_list = rest
            
            with open(file_path2,'w') as out:
                csv_out=csv.writer(out)
                csv_out.writerow(['Match No','Teams', 'Scores'])
                for row in data_list:
                    csv_out.writerow(row)
            print('Created the results.csv file')
      
def results_dataframe():
    directory= os.path.dirname(os.path.realpath(__file__))
    filename = "results.csv"
    file_path = os.path.join(directory,'clean/', filename)
    
    h = pd.read_csv(file_path) 
    print(h.head())
    h_list = []
    a_list = []
    mat = []
    sch = []
    sca = []
    hs = []
    ha = []
    score2 = h['Scores']
    team2 = h['Teams']
    num2 = h['Match No']
    
    
    for t in team2:
      
       j =  t.split('-')
       ht = j[0]
       at = j[1]
       h_list.append(ht)
       a_list.append(at)
    
    for s in score2:
       pattern = '\d'
       g = re.findall(pattern, str(s))
       FTHG = g[0]
       FTAG = g[1]
       HTHG = g[2]
       HTAG = g[3]
       sch.append(FTHG)
       sca.append(FTAG)
       hs.append(HTHG)
       ha.append(HTAG)
    for n in num2:
        
        mat.append(n)
          
    datta = zip(mat, h_list, a_list, sch, sca, hs, ha)
    tdata = list(datta)
    with open(file_path,'w') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['Match No','HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'HTHG', 'HTAG'])
        for row in tdata:
            csv_out.writerow(row)
    print("Updated the results dataframe and overrite the same file")

def match():
    with open('time.txt') as t:
      h =  t.readline()
      to = datetime.datetime.now()
      m = to.strftime("%b %d, %Y %H:%M")
      if m>h:
          extract()
          results_dataframe()
          final_s()
          os.remove('time.txt')
      else:
          print('no match so far, now waiting for another loop round')
def append_dict_as_row(file_name):
        directory= os.path.dirname(os.path.realpath(__file__))
        filename = "scrapedfile.csv"
        file_path_p= os.path.join(directory,'csvfiles/', filename)
    
        field_names = ['Match No', 'HomeTeam', 'AwayTeam', 'FTH', 'FTD','FTA',
                                   'FT1X', 'FTX2', 'HTH', 'HTD',  'HTA', 'HT1X', 'HTX2','U1.5', 'o1.5','U2.5',
                                   'o2.5','U3.5','o3.5','U4.5','o4.5','CS-1:0','CS-2:0','CS-2:1','CS-0:0', 'CS-1:1',
                                    'CS-2:2', 'CS-0:1','CS-0:2','CS-1:2','Other','BTS_Y','BTS_N','HT_YY', 'HT_NY']
        dict_of_elements = file_name
        
        # Open file in append mode
        with open(file_path_p, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            for row in dict_of_elements:
                print("added rows to scrapedfile")
                dict_writer.writerow(row)
        with open(file_path_p, 'rb') as file:
                data = file.read()
                file.close()
        
        file_data = pd.read_csv(file_path_p)
        file_path2 = os.path.join(directory,'clean')
        dr = file_data.drop_duplicates(subset=['Match No'], keep='first')
        with open(file_path_p) as f:
            g = f.readlines()
           
            if len(g) > 6:
                
                print('calling trim function')
                tim()
                shutil.move(file_path_p, file_path2)
            else:
                k = int (len (g))
                h = k-1
                print (f'We have {h} matches so far')
                print('unable to call trim function because matches arent greater than specified number yet')   
           
        if os.path.isfile('time.txt')==True:
            print('called match function')
            match()
            
            
        else:
            pass
            
            
def job():
        
        url = 'https://www.fortebet.ug/#/app/virtualsoccer'
        gDriver.get(url)
        time.sleep (3)
        page = gDriver.page_source
        soup = BeautifulSoup(page, 'lxml')                 
        tea_list = []
        tea = soup.find_all('div' ,class_="ng-binding")
        pattern = '[V][-|.]\S+\W[-]\W[V][-|.]\S+' 
        t = re.findall(pattern, str(tea))
                
        for i in t:
                       
                        h = i.replace('\\n\',', '')
                        pattern = '[-]'
                        te = re.split(pattern, h)                                            
                        if len(te) == 4:
                                HomeTeam = te[1]   #home team
                                HomeTeam.rstrip()
                                AwayTeam = te[-1]       #away team
                                AwayTeam = AwayTeam[:-7]
                                AwayTeam.rstrip()
                        else:
                                HomeTeam = te[0]   #home team
                                HomeTeam.rstrip()
                                AwayTeam = te[-1]
                                AwayTeam = AwayTeam[:-7]
                                AwayTeam.rstrip()
                        data = [HomeTeam, AwayTeam]     
                       
                        tea_list.append(data)
        odd = []
        mat_list = []
        odds1 = soup.find_all('div', class_='virtualsoccer-offer-match-header')
        
        
        for i in odds1:
                
                pattern4 = "(\d{1,3})" 
                h = re.findall(pattern4, str(i))
                mat_list.append(h[0])
    
            
        odds = soup.find_all('span' ,class_="ng-binding")
        for i in odds:
                    pattern = '\d+[.]\d+'
                    h = re.findall(pattern, str(i))
                    if len(h)>0:
                        odd.append(h[0])
                  
        set1 = odd[:32]
        set2 = odd[32:64]
        set3 = odd[64:97]
        odd = []
        odd.append(set1)
        odd.append(set2)
        odd.append(set3)
        
        res_list = []
        final = list(zip(mat_list, tea_list, odd))
               
        for i in final:
                   mat = i[0]
                   tea = i[1]
                   odds = i[2]
                   i = odds
                   
                    
                    
                   res = {     'Match No': mat, 'HomeTeam':tea[0], 'AwayTeam': tea[1], 'FTH': i[0], 'FTD': i[1], 'FTA': i[2],
                                   'FT1X': i[3], 'FTX2': i[4], 'HTH': i[5], 'HTD': i[6], 'HTA': i[7], 'HT1X': i[8], 'HTX2': i[9], 'U1.5': i[10], 'o1.5': i[11],
                                   'U2.5': i[12], 'o2.5': i[13], 'U3.5': i[14], 'o3.5': i[15], 'U4.5': i[16], 'o4.5': i[17], 'CS-1:0': i[18], 'CS-2:0': i[21],
                                   'CS-2:1': i[24], 'CS-0:0': i[19], 'CS-1:1': i[22],  'CS-2:2': i[25],  'CS-0:1': i[20],
                                   'CS-0:2': i[23],
                                   'CS-1:2': i[26],
                                   'Other': i[27],
                                   'BTS_Y':i[28],
                                   'BTS_N': i[30],
                                   'HT_YY': i[29],
                                   'HT_NY':i[31]   }
                   res_list.append(res)
        
        data=res_list 
        df=pd.DataFrame(data=data)
        if len(df.index) == 0:
            print("Scraped nothing but now retrying\n")
            job()
        else:
            print('All three matches scraped, now going ahead')
        
        directory = os.path.dirname(os.path.realpath(__file__))
        filename = "scrapedfile.csv"
        file_path = os.path.join(directory,'csvfiles/', filename)
        if os.path.isfile(file_path)==False:
            print("scrapedfile doesnot exists now creatig a new one")
            df.to_csv(file_path, index=False) 
            with open(file_path, 'rb') as file:
                data = file.read()
                file.close()  
        else:
            print('Now calling append dict as row since file exists')
            append_dict_as_row(data)
        if os.path.isfile(file_path)==True:
            f = pd.read_csv(file_path)
            print('file exists, checking if they make 60 records')
         
        else:
            pass
        
        file_path_c = os.path.join(directory,'clean/', filename)
        if os.path.isfile(file_path_c)==True:    
            print("File exists now calling final_s function")
            
            
        else:
            pass
        
        print('done')
        print(' ')


job()    
#    #schedule.every(15).minutes.do(job)
#    # schedule.every().hour.do(job)
#    #schedule.every().day.at('13:58').do(job)
#    #schedule.every(14).to(15).minutes.do(job)
#    # schedule.every().monday.do(job)
#    # schedule.every().wednesday.at("13:15").do(job)
schedule.every(13).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(13) # wait one minute
