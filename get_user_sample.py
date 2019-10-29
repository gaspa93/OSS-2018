import pandas as pd
import random

categories = {
    'watch': ['WatchEvent'],
    'interact': ['CommitCommentEvent', 'GollumEvent', 'PullRequestReviewCommentEvent','IssuesEvent', 
                 'IssueCommentEvent'],
    'contribute': ['CreateEvent', 'DeleteEvent', 'PullRequestEvent', 'PushEvent','ReleaseEvent', 'MemberEvent'],
    'fork': ['ForkEvent']
}

def define_category(event):
    for c in categories.keys():
        if event in categories[c]:
            return c

path = '../../HD2/2018/data/'
data = pd.read_csv(path + 'activity_2018_01.csv', header=None)
data.columns = ['source', 'target', 'event', 'created_at']
data['cat'] = data['event'].apply(lambda x: define_category(x))

N = 10000
users = data['source'].unique()
u_sample = random.sample(population=list(users), k=N)

u_df = data[data['source'].isin(u_sample)].groupby(by=['source', 'cat'])[['target']].count().reset_index()
u_df.columns = ['user', 'category', '#events']
print('01', u_df.shape[0])

for m in ['02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
    
    print('Processing month {}...'.format(m))
    
    # data import
    mdata = pd.read_csv(path + 'activity_2018_{}.csv'.format(m), header=None)
    mdata.columns = ['source', 'target', 'event', 'created_at']
    mdata['cat'] = mdata['event'].apply(lambda x: define_category(x))

    # extraxt activities of same sample of users
    sample_df = mdata[mdata['source'].isin(u_sample)].groupby(by=['source', 'cat'])[['target']].count().reset_index()
    sample_df.columns = ['user', 'category', '#events']
    print(m, sample_df.shape[0])
    
    # concatenate to other months
    u_df = pd.concat([u_df, sample_df], axis=0)
    
u_df.to_csv('sample_u_activity.csv', index=None)

