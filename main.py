from newCrawlerJobvision import CrawlJobsJobVision
from tqdm import tqdm
#pip install tqdm

if __name__ == '__main__':
    # step 1)
    # obj = CrawlJobsJobVision()
    # print(obj.find_companies(min_range=8000,max_range=8050))
    # step 2)
    #0 - 100_000
    min_range = 70
    max_range = min_range + 10
    #
    for i in tqdm(range(10)):
        obj = CrawlJobsJobVision(min_range=min_range,
                                 max_range=max_range)
        obj.run_threads()
        del obj
        max_range += 10
        min_range += 10

    # obj = CrawlJobsJobVision(min_range, max_range)
    # js = obj.send_request_to_api('168')
    # print(obj.parse_json_job(js))

    # print(obj.worker('33702'))

    # hint = """"
    #     number of iterations in the loop :
    #         10 : 100 next
    #         100 : 1000 next
    #         1000 : 10_000 next
    #         10_000 : 100_000
    #         .
    #         .
    #     example 1)
    #     if you want jobs between 5000 - 6000
    #     you should put the (min range = 5000) and (the range of iteration = 100)
    #     because they differ is 1000 and 100 range its mean min range + 1000
    #
    #     example 2)
    #     if you want jobs between 500_000 and 600_000
    #     you should put the (min range = 500_000) and (the range of iteration = 10_000)
    #     because they differ is 100_000 and 10_000 range its mean min range + 100_000
    # """

