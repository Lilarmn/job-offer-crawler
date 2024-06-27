import requests as rq
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from numpy import arange,argwhere,delete
from datetime import date ,timedelta ,datetime
import pyodbc
from random import choice
import json
from configs import *
import re


class CrawlJobsJobVision:
    def __init__(self,min_range=600_000,max_range=700_000):
        self.mainUrl = "https://jobvision.ir/jobs/"
        self.main_company_url = "https://jobvision.ir/companies/"
        self.number_of_jobs = arange(min_range,max_range)
        self.details = []
        self.company_details = []
        self.today_date = date.today()
        self.counter = 0

    @staticmethod
    def send_request(url):
        response = rq.get(url)
        if response.status_code == 200:
            return response.text
        else:
            return None

    @staticmethod
    def make_soup(html_doc):
        return BeautifulSoup(html_doc,'html.parser')

    def company_worker(self,company_number):
        url = self.main_company_url + str(company_number)
        if response := self.send_request(url):
            print(url,'--->',"\u2705")

            soup = BeautifulSoup(response,'html.parser')
            value = soup.select_one('body > app-root > div > app-company-details > section > div.row.py-3.justify-content-between > div.col-xs-12.col-sm-12.col-md-9.col-lg-9.px-0.pl-lg-3 > div.bg-white.rounded.shadow-sm > div > div.d-flex.apply-tabs > a:nth-child(2) > span:nth-child(2)')
            number_of_jobs = re.findall(r'\d+', value.text)[0]
            # name = soup.select_one('body > app-root > div > app-company-details > section > div.row.bg-white.rounded.shadow-sm > div > app-company-header > section > div.row.py-4.pr-3.ng-star-inserted > div.col-10.w-100.d-flex.align-items-end.justify-content-between > div.row.col-7.pr-0.py-1 > div:nth-child(1) > h1').text
            name = soup.select_one('body > app-root > div > app-company-details > section > div.row.bg-white.rounded.shadow-sm > div > app-company-header > section > div.row.py-4.pr-3.ng-star-inserted > div.col-10.w-100.d-flex.align-items-end.justify-content-between > div.row.col-7.pr-0.py-1 > div:nth-child(1) > label').text
            if website := soup.select_one('body > app-root > div > app-company-details > section > div.row.bg-white.rounded.shadow-sm > div > app-company-header > section > div.row.py-4.pr-3.ng-star-inserted > div.col-10.w-100.d-flex.align-items-end.justify-content-between > div.row.col-7.pr-0.py-1 > div.row.col-12.justify-content-start.py-2.pr-0 > a'):
                if 'نفر' not in website.text:
                    website = website.text
                else:
                    website = ''

            if company_size := soup.select_one('body > app-root > div > app-company-details > section > div.row.bg-white.rounded.shadow-sm > div > app-company-header > section > div.row.py-4.pr-3.ng-star-inserted > div.col-10.w-100.d-flex.align-items-end.justify-content-between > div.row.col-7.pr-0.py-1 > div.row.col-12.justify-content-start.py-2.pr-0 > span'):
                if 'نفر' in company_size.text:
                    company_size = company_size.text
                else:
                    company_size = ''

            print(url,'--->',name)
            self.company_details.append([url ,number_of_jobs ,name ,company_number,website ,company_size])
        else: print(url,'--->',"\u274C")

    def find_companies(self,min_range,max_range):
        max_threads = 10
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.company_worker, [i for i in range(min_range,max_range)])

        conn, cursor = self.connect_cursor()
        print(self.company_details)
        insert_query = "insert into [CrawlData].[dbo].[jobvision_companies] Values (?,?,?,?,?,?,?)"
        for company in self.company_details:
            cursor.execute(insert_query,
                           (company[0],
                            '1',
                            company[1],
                            company[2],
                            company[3],
                            company[4],
                            company[5]))
        cursor.commit()
        conn.commit()

        cursor.close()
        conn.close()

    def url_generator(self,job_number):
        return self.mainUrl + f'{job_number}' + '/'



    def parse_json_job(self,json_response: dict):
        jobsData = []
        if json_response['data']:
            for job in json_response['data']:
                data = dict()
                if not job.get("expireTime", {}).get('isExpired', None):
                    data["collect_date"] = str(date.today())
                    data['adv'] = job.get('company', {}).get('nameEn', None)
                    data['eng'] = job.get('company', {}).get('nameEn', None)
                    data['per'] = job.get('company', {}).get('nameFa', None)
                    data['field_company'] = job.get('industry', {}).get('title', None)
                    data['website'] = None
                    data['logo'] = job.get('company', {}).get('logoUrl', None)
                    data['aboutCompany'] = None
                    data['location'] = job.get('location', {}).get('province', {}).get('title', None)
                    data['jobTitle'] = job.get('title', None)
                    data['adDate'] = job.get("activationTime", {}).get('date', None)
                    data['adDate'] = datetime.fromisoformat(data['adDate'].replace('Z', '+00:00')).date()
                    data['link'] = self.mainUrl + str(job.get('id', None))
                    data['job_category'] = job.get('jobCategories', {})[0].get('titleFa', None)
                    data['workType'] = job.get('workType', {}).get('titleFa', None)
                    exp = job.get('properties', {}).get('requiredRelatedExperienceYears', None)
                    data['experience'] = exp if exp != str(0) else 'مهم نیست'
                    if job['properties']['salaryCanBeShown'] == 'false':
                        data['salary'] = job.get('salary', dict()).get('titleFa', None)
                    else:
                        data['salary'] = 'توافقی'
                    data['jobDescription'] = None
                    data['RequiredSkills'] = None  # fill later
                    data['Gender'] = job.get('gender', {}).get('titleFa', None)
                    data['MilitaryServiceStatus'] = None
                    data['MinEducationalQualification'] = None
                    data['Software'] = None
                    data['company_id'] = job.get('company', {}).get('id', None)
                    jobsData.append(data)

            return jobsData

    @staticmethod
    def parse_json_company(json_response: dict):
        data = dict()
        data['aboutCompany'] = json_response.get('data',dict()).get('descriptionFa',None)
        data['website'] = json_response.get('data',dict()).get('websiteAddress',None)
        return data


    @staticmethod
    def send_request_to_api(company_number: str):
        base_url = 'https://candidateapi.jobvision.ir/api/v1/JobPost/GetListOfCompanyJobPosts?companyId='
        response = rq.get(base_url + str(company_number))
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            return None

    @staticmethod
    def send_request_to_api_company(company_number: str):
        base_url = 'https://candidateapi.jobvision.ir/api/v1/Company/Details?companyId='
        response = rq.get(base_url + str(company_number))
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            return None

    def parse_job(self,data:dict):
        url = data.get('link',None)
        company_dict = self.send_request_to_api_company(data.get('company_id'))
        residual_dict = self.parse_json_company(company_dict)
        data.update(residual_dict)
        html_doc = self.send_request(url)
        soup = self.make_soup(html_doc)
        if x:= soup.select_one('body > app-root > div > app-job-detail-external > section > div > div > div.ng-tns-c695458075-22.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div:nth-child(2) > div > div'):
            data['jobDescription'] = str(x)
        elif x := soup.select('body > app-root > div > app-job-detail-external > section > div > div > div.ng-tns-c19522971-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div:nth-child(2)'):
            data['jobDescription'] = str(x)
        else:
            x = soup.find('div',attrs={'class':'col px-0 mr-2'})
            data['jobDescription'] = str(x)



        base_selector_path = "body > app-root > div > app-job-detail-external > section > div.container.py-lg-2.px-0.ng-tns-c1381458355-5 > div > div.ng-tns-c1381458355-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div.col-12.row.px-0.mb-4 > div > div:nth-child(6) > div.col-12.col-lg-9.px-lg-0 > div > div > app-tag:nth-child(XXX) > span > span.tag-title.font-weight-bold"
        softwares = ''
        counter = 1
        while True:
            path = base_selector_path.replace('XXX', str(counter))

            if new_software := soup.select_one(path):
                try:
                    softwares = softwares + ' / ' + new_software.text
                except TypeError:
                    break
            else:
                break
            counter += 1

        data['Software'] = softwares[1:]

        if study := soup.find('span', class_='tag-title font-weight-bold'):
            data['MinEducationalQualification'] = study.text
        else:
            data['MinEducationalQualification'] = 'مهم نیست'

        try:
            gender, data['MilitaryServiceStatus'] = [x.text for x in
                                soup.find_all('div', class_='requirement-value text-black bg-light py-2 px-3')]
        except ValueError:
            gender = 'مهم نیست'
            data['MilitaryServiceStatus'] = 'مهم نیست'

        if language := soup.select_one(
                'body > app-root > div > app-job-detail-external > section > div.container.py-lg-2.px-0.ng-tns-c1381458355-5 > div > div.ng-tns-c1381458355-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div.col-12.row.px-0.mb-4 > div > div:nth-child(5) > div.col-12.col-lg-9.px-lg-0 > div > div > app-tag > span > span.tag-title.font-weight-bold'):
            language = language.text
            if grade := soup.select_one(
                    'body > app-root > div > app-job-detail-external > section > div.container.py-lg-2.px-0.ng-tns-c1381458355-5 > div > div.ng-tns-c1381458355-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div.col-12.row.px-0.mb-4 > div > div:nth-child(5) > div.col-12.col-lg-9.px-lg-0 > div > div > app-tag > span > span.tag-value.ng-star-inserted'):
                grade = grade.text
                data['lang_grade'] = language + ' ' + grade
            else:
                data['lang_grade'] = 'مهم نیست'
        else:
            data['lang_grade'] = 'مهم نیست'

        if skills := soup.select_one('body > app-root > div > app-job-detail-external > section > div > div > div.ng-tns-c695458075-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div.col-12.row.px-0.mb-4 > div'):
            data['RequiredSkills'] = str(skills)
        elif skills := soup.find('col-12 row px-0'):
            data['RequiredSkills'] = str(skills)
        elif skills := soup.select_one('body > app-root > div > app-job-detail-external > section > div > div > div.ng-tns-c19522971-5.col-lg-9.job-detail-external.pr-lg-0.ng-star-inserted > div:nth-child(3) > div > div > app-job-specification > section > div.col-12.row.px-0.mb-4 > div'):
            data['RequiredSkills'] = str(skills)
        else:
            skills = 'مهم نیست'


        return data


    @staticmethod
    def connect_cursor():
        conn = pyodbc.connect(f'DRIVER={DRIVER};' +
                              f'SERVER={SERVER};' +
                              f'DATABASE={DATABASE};' +
                              f'UID={UID};' +
                              f'PWD={PWD}')
        cursor = conn.cursor()
        return conn, cursor

    def worker(self,company_number):
        print('worker ' ,self.counter ,' start')
        api_response = self.send_request_to_api(company_number)
        if api_response:
            half_dicts = self.parse_json_job(api_response)
            # print(half_dicts)
            if half_dicts:
                for data in half_dicts:
                    new_data = self.parse_job(data)
                    self.details.append(new_data)
                    # print(new_data)

        self.counter +=1
        print('worker ', self.counter, ' stop' )


    def run_threads(self):
        max_threads = 10
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(self.worker, [str(number) for number in self.number_of_jobs])

        conn, cursor = self.connect_cursor()
        insert_query = '''
                         INSERT INTO [CrawlData].[dbo].[JobAds_jobvision] ([CollectionDate], [AdvertiserName], [EnglishName], [PersianName], [FieldOfActivity], [CompanyWebsite], [LogoFileAddress], [AboutCompany], [Location], [AdTitle], [AdRegistrationDate], [Link], [JobCategory], [CollaborationType], [MinWorkExperience], [Salary], [JobDescription], [RequiredSkills], [Gender], [MilitaryServiceStatus], [MinEducationalQualification], [Software] ,[company_id],[ShetabCompanyId],[ShetabId])
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          --VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                         '''
        # insert_query = """
        # INSERT INTO [CrawlData].[dbo].[JobAds_jobvision] ([CollectionDate], [AdvertiserName], [EnglishName], [PersianName], [FieldOfActivity], [CompanyWebsite], [LogoFileAddress], [AboutCompany], [Location], [AdTitle], [AdRegistrationDate], [Link], [JobCategory], [CollaborationType], [MinWorkExperience], [Salary], [JobDescription], [RequiredSkills], [Gender], [MilitaryServiceStatus], [MinEducationalQualification], [Software], [company_id])
        # VALUES (N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?', N'?')
        # """



        for detail in self.details:
            print(detail.get('link', None))
            edu = detail.get("MinEducationalQualification", None)
            edu = edu if edu in ['کاردانی','کارشناسی','کارشناسی ارشد','دیپلم','دکترا'] else 'مهم نیست'
            military = detail.get("MilitaryServiceStatus",None)
            military = military if "سربازی" in military else 'مهم نیست'
            software = str(detail.get('Software', None))[1:]
            cursor.execute(
                insert_query,
                (
                    detail.get('collect_date',None),
                    detail.get('adv', None),
                    detail.get('eng', None),
                    detail.get('per', None),
                    detail.get('field_company', None),
                    detail.get('website', None),
                    detail.get('logo', None),
                    detail.get('aboutCompany', None),
                    detail.get('location', None),
                    detail.get('jobTitle', None),
                    detail.get('adDate', None),
                    detail.get('link', None),
                    detail.get('job_category', None),
                    detail.get('workType', None),
                    detail.get('experience', None),
                    detail.get('salary', None),
                    detail.get('jobDescription', None),
                    detail.get('RequiredSkills',None),
                    detail.get('Gender', None),
                    military,
                    edu,
                    software,
                    detail.get("company_id", None),
                    None,
                    None
                )
            )
        cursor.commit()
        conn.commit()

        cursor.close()
        conn.close()