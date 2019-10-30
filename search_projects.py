from github import Github
import os, sys, time
import csv
import pandas as pd


def filterString(string):
    strOut = string.replace('\r',' ').replace('\n',' ').replace('\t',' ').encode('utf-8')
    return strOut

def save_project_data(repo):
    id_github = repo.id
    fullname = repo.full_name
    timestamp = repo.created_at
    size = repo.size
    w_count = repo.watchers_count
    stars = repo.stargazers_count
    url = repo.html_url

    try:
        desc = filterString(repo.description)
    except:
        desc = ''

    pwriter.writerow([id_github, fullname, timestamp, size, w_count, stars, desc, url])

# check after each API call to avoid exception (and so skipping some data)
def waitAPILimitReset():
    if g.rate_limiting[0] == 0:
        sec_to_wait = g.rate_limiting_resettime-time.time()
        print('API Rate Limit reached!')
        print('Waiting for {} min'.format(sec_to_wait/60))

        time.sleep(abs(sec_to_wait))

API_KEY = open('key.txt', 'r').read()
g = Github(API_KEY)

min_stars = 100
max_stars = 4011
creation_date_up = '2019-01-01'
creation_date_down = '2017-01-01'
query = 'stars:<{} created:>{}'.format(max_stars, creation_date_down)

with open('2018_top_projects.csv', 'a') as pfile:
    pwriter = csv.writer(pfile, quoting = csv.QUOTE_MINIMAL, delimiter=',')
    #pwriter.writerow(['id_project','name', 'created_at', 'size', 'watchers_count', 'stars','description', 'url'])

    repositories = g.search_repositories(query, 'stars', 'desc')
    for repo in repositories:
        save_project_data(repo)
        waitAPILimitReset()
