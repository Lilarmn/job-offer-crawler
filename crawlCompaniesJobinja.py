from bs4 import BeautifulSoup
import requests as rq
from concurrent.futures import ThreadPoolExecutor
from datetime import date , timedelta
import pyodbc
from configs import *
import re
from itertools import groupby

class CrawlCompanies:
    def __init__(self,url,number_of_page):
        self.mainUrl = url
        self.links = []
        self.n_page = number_of_page
        self.counter = 0

    @staticmethod
    def send_request(url):
        response = rq.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print('request failed')
            return None
    def parse_page(self,html_doc):
        print(f'{self.counter} pages start')
        soup = BeautifulSoup(html_doc ,'html.parser')

        links_in_page = soup.find_all('a',{
            'class':'c-companyOverview'
        })
        links_in_page = list(map(lambda x:x.get('href') ,links_in_page))

        # def check_value_of_links(url):
        #
        #     if url.split('/')[-1] != 'jobs':
        #         return True
        #     else:
        #         return False
        # links_in_page = list(filter(lambda x:check_value_of_links(x),links_in_page))

        # def find_about(url):
        #     if url.split('/')[-1] == 'jobs':
        #         about_url = url.split('/')[:-1]
        #     about_url = url + '/about'
        #     html_doc_about = self.send_request(about_url)
        #     about_soup = BeautifulSoup(html_doc_about, 'html.parser')
        #     about = about_soup.find('div',class_='c-cardText__body').text
        #     about = about.replace('\u200c','')
        #     about = about.replace('\n',' ')
        #     return about

        # abouts = [find_about(x) for x in links_in_page]

        self.links.extend((links_in_page,))
        # self.links.update(dict(zip(links_in_page,abouts)))
        self.counter += 1
        print(f'{self.counter} pages finish ------')

    def worker(self,url):
        html_doc = self.send_request(url)
        if html_doc:
            self.parse_page(html_doc)
        else:
            print(f'{url} failed')

    @staticmethod
    def connect_cursor():
        conn = pyodbc.connect(f'DRIVER={DRIVER};' +
                              f'SERVER={SERVER};'+
                              f'DATABASE={DATABASE};'+
                              f'UID={UID};'+
                              f'PWD={PWD}')
        cursor = conn.cursor()
        return conn ,cursor

    def run_threads(self):

        max_threads = 10
        urls = [f"{self.mainUrl}?page={i}" for i in range(1, self.n_page + 1)]

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.worker, urls)

        conn,cursor = self.connect_cursor()
        # query = """
        #         INSERT INTO CrawlData..jobinja_companies
        #         VALUES (?)
        #     """
        query = """
                INSERT INTO CrawlData..[jobinja_link]
                SELECT ?,?
                WHERE NOT EXISTS (
                    SELECT 1 FROM CrawlData..[jobinja_link] WHERE link = ?
                )
            """
        for links in self.links:
            for link in links:
                if link.split('/')[-1] != 'jobs':
                    company_id = link.split('/')[-1]
                else:
                    company_id = link.split('/')[-2]
                cursor.execute(query,(link,company_id,link,))

        cursor.commit()
        conn.close()

class CrawlCompanyDetail(CrawlCompanies):
    def __init__(self):
        super().__init__('#', 1)
        self.counter = 0

    def select_links(self,number_of_companies):
        conn , curser = self.connect_cursor()
        query = "select top(?) link from CrawlData..jobinja_link"
        curser.execute(query,(number_of_companies,))
        links = curser.fetchall()
        links = list(map(lambda x:x[0], links))
        curser.close()
        conn.close()
        return links

    def worker(self,link):
        if link.split('/')[-1] != 'jobs':
            link = link + "/jobs"
        response = self.send_request(link)
        self.counter += 1
        print(self.counter,'thread start')
        if response:
            soup = BeautifulSoup(response ,'html.parser')
            headers = soup.find('div',class_='c-companyHeader__meta').text.strip().split('\n')
            headers= list(filter(lambda x:x if x else None,headers))
            year,size,website,category = None,None,None,None
            for header in headers:
                if 'تاسیس' in header:
                    year = header
                elif 'نفر' in header:
                    size = header
                elif '.' in header:
                    website = header
                else:
                    category = header

            name = soup.find('h2',class_='c-companyHeader__name').text
            number_of_jobs = soup.find('span',class_="c-companyHeader__navigatorOpenJobs").text

            about = soup.find('div',class_='c-cardText__body')
            try:
                if about.text:
                    about = about.text
                    if about:
                        about = about.strip().replace('\n',' ')
            except AttributeError:
                about = None
            status = 'فعال' if number_of_jobs != '۰' else 'بدون آگهی'

            print((link,status,number_of_jobs,name,link.split('/')[-2],website,size))

            conn, curser = self.connect_cursor()
            query = """insert into [CrawlData]..[jobinja_companies]
                                    values(?,?,?,?,?,?,?,?,?,?)"""
            curser.execute(query,
                           (link,
                            status,
                            number_of_jobs,
                            name,
                            link.split('/')[-2],
                            website,
                            size,
                            about,
                            year,
                            category))
            conn.commit()  # Commit the changes to the database connection, not the cursor
            curser.close()
            conn.close()
            print(self.counter, 'thread Stop')
        else:
            return None

    def find_companies_details(self,number_of_companies):
        max_threads = 10
        links = self.select_links(number_of_companies)
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.worker,links)
            # tqdm(executor.map(self.worker, links), total=len(links), desc="Processing links")


class CrawlPage(CrawlCompanies):
    def __init__(self,number_of_companies):
        super().__init__('#',1)
        self.job_details = []
        self.today_date = date.today()
        self.number_of_companies = number_of_companies
        self.job_counter = 0

    def find_all_jobs_links(self,company_url):

        html_doc = self.send_request(company_url)
        soup = BeautifulSoup(html_doc , 'html.parser')
        job_links = soup.find_all('a',class_='c-jobListView__titleLink')
        job_links = list(map(lambda x:x.get('href') , job_links))
        job_dates = soup.find_all('span',class_="c-jobListView__passedDays")
        job_dates = list(map(lambda x: x.text.strip(), job_dates))

        return dict(zip(job_links,job_dates))

    def parse_job_page(self,job_link):

        html_doc = self.send_request(job_link)
        soup = BeautifulSoup(html_doc, 'html.parser')
        if soup.find('div',class_="c-alert c-alert--error c-alert--block c-alert--textCenter"):
            return None
        if soup.find('div',class_='tags') and soup.find('h4',class_='c-infoBox__itemTitle'):

            company = job_link.split('/')[4]

            job_title = soup.find('div',
                                        class_='c-jobView__titleText').text.replace('\n','').strip()

            tags_title = soup.find_all('h4', class_='c-infoBox__itemTitle')
            tags_title = list(map(lambda x: x.text.replace('\n','').strip().replace('\u200c',' '), tags_title))

            tags_results = soup.find_all('div',class_='tags')
            tags_results = list(map(lambda x: x.text.replace('\n','').strip().replace('\u200c',' '), tags_results))

            tags_dict2 = dict(zip(tags_title,tags_results))

            tags_dict2['موقعیت مکانی'] = tags_dict2['موقعیت مکانی'].split(' ')[0]

            base_selector = "#singleJob > div.c-forceToLogin__content > div > div.col-md-8.col-sm-12 > section > ul.c-infoBox.u-mB0 > li:nth-child(1) > div > span:nth-child(HERE_PUT_NUMBER)"
            # I can do it with format function but just for fun :)
            if element := soup.select_one(base_selector.replace('HERE_PUT_NUMBER','1')):
                element = element.text
                if soup.select_one(base_selector.replace('HERE_PUT_NUMBER','2')):
                    element = element + ' / ' +  soup.select_one(base_selector.replace('HERE_PUT_NUMBER','2')).text
                    if soup.select_one(base_selector.replace('HERE_PUT_NUMBER', '3')):
                        element = element + ' / ' + soup.select_one(base_selector.replace('HERE_PUT_NUMBER', '3')).text
                        tags_dict2['مهارت های مورد نیاز'] = element
                else:
                    tags_dict2['مهارت های مورد نیاز'] = element

            company_persian_name = soup.find('h2',class_='c-companyHeader__name').text
            tags_dict2['pr_name'] = company_persian_name

            print(tags_dict2)

            if company_type := soup.select_one(
                    '#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(2) > a'):
                company_type = company_type.text
                tags_dict2['type_comp'] = company_type
            else:
                if company_type := soup.select_one(
                        '#view-job > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--2.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(1) > a'):
                    company_type = company_type.text
                    tags_dict2['type_comp'] = company_type

            if company_website := soup.select_one(
                    '#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(4) > a'):
                company_website = company_website.text
                tags_dict2['website'] = company_website
            elif company_website := soup.select_one('#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(3) > a'):
                company_website = company_website.text
                tags_dict2['website'] = company_website
            else:
                if company_website := soup.select_one(
                    '#view-job > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--2.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(3) > a'):
                    company_website = company_website.text
                    tags_dict2['website'] = company_website

            if company_size := soup.select_one(
                '#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(3)'):
                if 'نفر' in company_size.text:
                    company_size = company_size.text
                    tags_dict2['size'] = company_size
                else:
                    if company_size := soup.select_one(
                            '#view-job > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--2.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(2)'):
                        company_size = company_size.text
                        if 'نفر' in company_size:
                            tags_dict2['size'] = company_size
                        else:
                            tags_dict2['website'] = company_size
                            if company_size := soup.select_one(
                                '#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(2)'
                                                             ):
                                company_size = company_size.text
                                if  'نفر' in company_size:
                                    tags_dict2['size'] = company_size
                                else:
                                    tags_dict2['website'] = company_size
                                    if company_size := soup.select_one('#view-job > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(2)'):
                                        if 'نفر' in company_size.text:
                                            tags_dict2['size'] = company_size.text
                                        else:
                                            tags_dict2['website'] = company_size.text
                                            tags_dict2['size'] = ''


            img_tag = soup.find('img', class_='c-companyHeader__logoImage')['src']
            tags_dict2['logo'] = img_tag

            # if job_description := soup.find('div',class_="o-box__text s-jobDesc c-pr40p"):
            #     job_description = job_description.get_text()
            #     tags_dict2['descr'] = job_description
            # else:
            #     if job_description := soup.find('div', class_="o-box__text s-jobDesc c-pr40p"):
            #         # job_description = job_description.get_text()
            #         tags_dict2['descr'] = 'job_description'
            if job_description := soup.select_one(
                '#singleJob > div.c-forceToLogin__content > div > div.col-md-8.col-sm-12 > section > div.o-box__text.s-jobDesc.c-pr40p'):
                job_description = str(job_description).replace('<div class="o-box__text s-jobDesc c-pr40p">', '')
                tags_dict2['descr'] = job_description

            if company_description := soup.select_one("#singleJob > div.c-forceToLogin__content > div > div.col-md-8.col-sm-12 > section > div:nth-child(6)"):
                tags_dict2['about'] = company_description.text

            return company ,job_title ,tags_dict2 ,job_link

    def get_links_from_database(self):
        conn ,cursor = self.connect_cursor()
        query = """
            select top(?)link from CrawlData..[jobinja_link]
        """
        cursor.execute(query,(self.number_of_companies,))
        links = cursor.fetchall()
        cursor.close()
        conn.close()
        return list(map(lambda x:x[0] ,links))

    @staticmethod
    def make_filter_dictionary(links):
        keys = list(map(lambda x: x.split('/')[4], links))
        values = list(map(lambda x: x.split('/')[6], links))
        data = list(zip(keys, values))
        return {k: [v for _, v in g] for k, g in groupby(sorted(data), key=lambda x: x[0])}


    def worker(self,url:str):
        if url.split('/')[-1] != 'jobs':
            url = url + '/jobs'
        jobs = self.find_all_jobs_links(url)
        for job,dateTime in jobs.items():
            if dateTime != '(منقضی شده)':
                print(self.job_counter)
                detail = self.parse_job_page(job) + (dateTime,)
                print(detail)
                self.job_details.append(detail)
                self.job_counter += 1

    def run_threads(self):
        max_threads = 10
        urls = self.get_links_from_database()
        print(urls)
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.worker, urls)

        conn, cursor = self.connect_cursor()
        insert_query = '''
                         INSERT INTO [CrawlData].[dbo].[JobAds_jobinja] ([CollectionDate], [AdvertiserName], [EnglishName], [PersianName], [FieldOfActivity], [CompanyWebsite], [LogoFileAddress], [OrganizationSize], [AboutCompany], [Location], [AdTitle], [AdRegistrationDate], [Link], [JobCategory], [CollaborationType], [MinWorkExperience], [Salary], [JobDescription], [RequiredSkills], [Gender], [MilitaryServiceStatus], [MinEducationalQualification], [Software] ,[company_id])
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        '''

        check_query = "select link from [CrawlData].[dbo].[JobAds_jobinja]"
        cursor.execute(check_query)
        existed_jobs =  cursor.fetchall()
        existed_jobs = list(map(lambda x:x[0],existed_jobs))
        grouped_data  = self.make_filter_dictionary(existed_jobs)
        del existed_jobs
        

        def check_condition(cmp_id,jb_id):
            if cmp_id not in grouped_data.keys():
                return True
            if jb_id not in grouped_data[cmp_id]:
                return True
            return False

        for detail in self.job_details:
            delta = detail[-1]
            if delta:
                try:
                    number = int(re.search(r'\d+', delta).group())
                    new_date = self.today_date - timedelta(days=number)
                except:
                    print(delta)
                    new_date = None
            else:
                new_date = None

            company_id = detail[3].split('/')[-4]
            job_id = detail[3].split('/')[6]

            if check_condition(company_id,job_id):
                cursor.execute(
                    insert_query,
                    (
                        str(date.today()),
                        'jobinja',
                        detail[0],
                        detail[2].get('pr_name', None).split('|')[0],
                        detail[2].get('type_comp', None),
                        detail[2].get('website', None),
                        detail[2].get('logo', None),
                        detail[2].get('size', None),
                        detail[2].get('about', None),
                        detail[2].get('موقعیت مکانی', None),
                        detail[1],
                        new_date,
                        detail[3],
                        detail[2].get('دسته بندی شغلی', None),
                        detail[2].get('نوع همکاری', None),
                        detail[2].get('حداقل سابقه کار', None),
                        detail[2].get('حقوق', None),
                        detail[2].get('descr', None),
                        detail[2].get('مهارت های مورد نیاز', None),
                        detail[2].get('جنسیت', None),
                        detail[2].get('وضعیت نظام وظیفه', None),
                        detail[2].get('حداقل مدرک تحصیلی', None),
                        None,
                        company_id
                            )
                    )
                print('one row added')

        cursor.commit()
        conn.commit()

        # delete_query = """
        #             delete top(?) from CrawlData..jobinja_companies
        #         """
        # cursor.execute(delete_query, (self.number_of_companies,))
        #
        # cursor.commit()
        # conn.commit()

        cursor.close()
        conn.close()
