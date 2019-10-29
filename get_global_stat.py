import pandas as pd

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
outpath = 'aggregated/'

for m in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
    
    print('Processing month {}...'.format(m))
    
    # data import
    mdata = pd.read_csv(path + 'activity_2018_{}.csv'.format(m), header=None)
    mdata.columns = ['source', 'target', 'event', 'created_at']
    mdata['cat'] = mdata['event'].apply(lambda x: define_category(x))

    # export global events and categories counts
    mdata['cat'].value_counts().reset_index().to_csv(outpath + 'category_counts_{}.csv'.format(m), index=None)
    mdata['event'].value_counts().reset_index().to_csv(outpath + 'event_counts_{}.csv'.format(m), index=None)