from crawlCompaniesJobinja import *

if __name__ == '__main__':
    # step one )
    # obj = CrawlCompanies('https://jobinja.ir/companies', 100)
    # print(obj.run_threads())

    # step two)
    obj = CrawlPage(10)
    print(obj.run_threads())

    # step three)
    # obj = CrawlCompanyDetail()
    # print(obj.worker("https://jobinja.ir/companies/galaxyvisiontop-gmail-com/jobs"))
    # print(obj.select_links(500))
    # print(obj.find_companies_details(178))
    # print(obj.select_links(178))