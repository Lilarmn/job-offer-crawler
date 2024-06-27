import requests as rq
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from configs import *
import pyodbc
import re

class FindEmails:

    def __init__(self):
        self.data = dict()

    def getWebsites(self):
        conn,cursor = self.connect_cursor()
        query = "select distinct CompanyWebsite from [CrawlData].[dbo].[JobAds_jobinja]"
        cursor.execute(query)
        Websites = cursor.fetchall()
        Websites = list(map(lambda x: self.urlCleaner(x[0]),Websites))
        return Websites[1:]

    @staticmethod
    def urlCleaner(url:str):
        if url:
            if 'www' in url:
                url = url[4:]
            url = url.replace('/','')
            return url
        else: return None

    @staticmethod
    def connect_cursor():
        conn = pyodbc.connect(f'DRIVER={DRIVER};' +
                              f'SERVER={SERVER};'+
                              f'DATABASE={DATABASE};'+
                              f'UID={UID};'+
                              f'PWD={PWD}')
        cursor = conn.cursor()
        return conn ,cursor

    @staticmethod
    def send_request(url):
        try:
            for path in ['/contact','/contact-us','/تماس-با-ما','/about-us']:
                response = rq.get('http://' + url + path)
                if response.status_code == 200:
                    print('http://' + url + path)
                    return response.text
                elif response := rq.get('https://' + url + path):
                    print('https://' + url + path)
                    if response.status_code == 200:
                        return response.text
            else:
                print("Request Error")
                return None
        except rq.exceptions.MissingSchema:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None
        except rq.exceptions.ConnectTimeout:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None
        except rq.exceptions.ConnectionError:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None
        except rq.exceptions.ReadTimeout:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None
        except rq.exceptions.ChunkedEncodingError:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None
        except rq.exceptions.ContentDecodingError:
            print(url, ' ', "\U0001F5D9")
            print('Request Error')
            return None

    @staticmethod
    def makeSoup(Res):
        try:
            return BeautifulSoup(Res,'html.parser')
        except TypeError:
            return None

    def findEmail(self,Soup:BeautifulSoup,url):

        emails = []
        email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
        email_pattern2 = re.compile(r"[\w.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
        for tag in Soup.find_all(["a","span","i","br","div",'strong',"p"]):
            if tag.string:
                matches = email_pattern.search(tag.string)
                # matches = email_pattern.findall(tag.string)
                if matches:
                    emails.append(matches[0])
                else:
                    matches = email_pattern2.search(tag.string)
                    if matches:
                        emails.append(matches[0])
        self.data[url] = ','.join(list(set(emails)))
        print(url, '--->' ,emails)


    def worker(self,url):
        response = self.send_request(url)
        if response :
            soup = self.makeSoup(response)
            self.findEmail(Soup=soup,url=url)

    def runThreads(self):
        max_threads = 10
        urls = self.getWebsites()
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.worker, urls)

