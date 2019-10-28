from github import Github
import os
import sys
import re
import pandas as pd
import time
import csv

def writeUserdata(u):
	if u.name is not None:
		name = u.name.encode('utf-8')
	else:
		name = None
	if u.company is not None:
		company = u.company.encode('utf-8')
	else:
		company = None
	if u.location is not None:
		location = u.location.encode('utf-8')
	else:
		location = None
	writer.writerow([u.id, u.login, name, company,location, u.email,u.public_repos,u.followers,u.following,u.created_at,u.updated_at])
	u_ids.append(u.id)

# check after each API call to avoid exception (and so skipping some data)		
def waitAPILimitReset():
	if g.rate_limiting[0] == 0:
		sec_to_wait = g.rate_limiting_resettime-time.time()
		print 'API Rate Limit reached!'
		print 'Waiting for {} min'.format(sec_to_wait/60)
		
		time.sleep(sec_to_wait)

#create a Github instance using username and password
g = Github(sys.argv[1], sys.argv[2])
print g.rate_limiting, (g.rate_limiting_resettime-time.time())/60

random_u = pd.read_csv('analysis-users/activity_random_users.csv')
random_u = random_u[['source']].drop_duplicates()
u = pd.read_csv('data/user.csv')
userlist = random_u.merge(u, left_on='source', right_on='id').drop_duplicates()

N = userlist.shape[0]
print 'Number of users for GitHub API query: {}'.format(N)
 
u_ids = list(pd.read_csv('data/user_complete.csv')['id_user']) # metadata already available 

start = time.time()
with open('data/user_complete.csv', 'a') as userfile:
	writer = csv.writer(userfile, quoting = csv.QUOTE_MINIMAL)
	#followfile.write('id_follower,id_followed\n')
	#userfile.write('id_user,login,name,company,location,email,public_repos,followers,following,created_at,updated_at\n')
	for index, u in userlist.iterrows():
		if int(u['id']) not in u_ids:
			try:
				userobject = g.get_user(u['login'])
				writeUserdata(userobject)
				
				waitAPILimitReset()
				
				'''
				if userobject.followers > 0:
					followers = userobject.get_followers()
					
					for target in followers:
					   waitAPILimitReset()
					
					   followfile.write('{},{}\n'.format(target.id, userobject.id))
					   writeUserdata(target)
				'''
					   
			except Exception as e:
				print u['id'], u['login'], e
				
		print 'Completion: {:.1f} %'.format(float(index+1)*100/N)

print 'Time needed: {} hours'.format((time.time()-start)/3600)