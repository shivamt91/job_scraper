import requests
import re
import time
import json
import concurrent.futures
from bs4 import BeautifulSoup


def my_main():
    start = time.perf_counter()
    print('Starting our Spider!\n')
    url = 'https://www.indeed.co.in/jobs?q=software+developer&l=Bengaluru%2C+Karnataka&start='
    total_jobs_to_scrape = 1000
    jobs = []
    page = 0
    count = 1

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = []
        while len(jobs) < total_jobs_to_scrape:
            print('Scraping page: ', count)
            my_url = url + str(page)
            results.append(executor.submit(job_scraper, my_url))

            for f in concurrent.futures.as_completed(results):
                new_jobs = f.result()
                jobs.extend(new_jobs)
                print('Jobs scraped: ', len(new_jobs), '\n')
                print('Total jobs scraped so far: ', len(jobs))
                page += 10
                count += 1

    print('Total jobs scraped: ', len(jobs), '\n')
    print('\n---Removing extra jobs(if any)---\n')
    jobs = jobs[:total_jobs_to_scrape]

    print('Sorting the jobs based on the salaries...\n(note: jobs without salaries will be added to the bottom of the list!)\n')
    jobs_with_salary = []
    jobs_without_salary = []
    for job in jobs:
        if 'salary' in job.keys():
            jobs_with_salary.append(job)
        else:
            jobs_without_salary.append(job)

    sorted_jobs = sorted(jobs_with_salary, key=lambda j: j['salary'], reverse=True)
    sorted_jobs.extend(jobs_without_salary)

    file_name = 'my_json_file'
    print('Writing data to: ', file_name, '\n')
    with open(file_name + '.json', 'w') as f:
        json.dump(sorted_jobs, f)

    print('\nExiting Spider...')

    finish = time.perf_counter()
    print('Finished in ', round(finish-start, 2), ' seconds')


def job_scraper(my_url):
    response = requests.get(url=my_url)
    soup = BeautifulSoup(response.text, features='html.parser')

    my_list = []
    for job in soup.select('.jobsearch-SerpJobCard'):
        my_dict = dict({
            "title": job.select_one('.title > a').get('title'),
        })

        summary = [i.string for i in job.select('.summary > ul > li')]
        my_dict['summary'] = ''
        for i in summary:
            if i:
                my_dict['summary'] = my_dict['summary'] + i + ' '

        company = job.select_one('.sjcl > div > .company').string
        if company:
            my_dict['company'] = company.replace('\n', '')

        salary = job.select_one('.salarySnippet > span > .salaryText')
        if salary:
            salary = salary.string.replace('\n', '')
            if (' - ' in salary or 'to' in salary) and 'year' in salary:
                my_dict['salary'] = int(re.search('- ₹([\d,]+)', salary).group(1).replace(',', ''))
            elif 'year' in salary:
                my_dict['salary'] = int(re.search('₹([\d,]+)', salary).group(1).replace(',', ''))
            elif (' - ' in salary or 'to' in salary) and 'month' in salary:
                my_dict['salary'] = int(re.search('- ₹([\d,]+)', salary).group(1).replace(',', '')) * 12
            elif 'month' in salary:
                my_dict['salary'] = int(re.search('₹([\d,]+)', salary).group(1).replace(',', '')) * 12

        date_posted = job.select_one('.jobsearch-SerpJobCard-footer > div > div > div > .date')
        if date_posted:
            my_dict["date_posted"] = date_posted.string

        is_sponsored = job.select_one('.jobsearch-SerpJobCard-footer > div > div > div > .sponsoredGray')
        if is_sponsored:
            my_dict['is_sponsored'] = is_sponsored.string

        my_list.append(my_dict)

    return my_list


my_main()
