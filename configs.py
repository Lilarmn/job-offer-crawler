DRIVER = '{ODBC Driver 17 for SQL Server}'
SERVER = '.'
DATABASE = 'CrawlData'
UID = 'sa'
PWD = 'armanjt7'

website_selectors = [
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(4) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--1.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(3) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--4.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(3) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--2.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(3) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--4.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(4) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(3) > a',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--5.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(3) > a',
    ]

company_size_selectors = [
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--3.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(2)',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--1.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(2)',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--5.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(2)',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(3)',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--simple > div.c-companyHeader__cover.c-companyHeader__cover--2.c-companyHeader__cover--noBg > div > div > div > div > span:nth-child(2)',
    '#company-jobs > div > div.c-companyHeader.c-companyHeader--premium > div.c-companyHeader__cover > div > div > div > div > span:nth-child(2)',
]


# about crawling
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