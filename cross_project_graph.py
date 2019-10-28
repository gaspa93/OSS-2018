import pandas as pd
import numpy as np
import time
import multiprocessing
from joblib import Parallel, delayed

categories = {
    'watch': ['WatchEvent'],
    'interact': ['CommitCommentEvent', 'GollumEvent', 'PullRequestReviewCommentEvent','IssuesEvent', 'IssueCommentEvent',
                'ForkEvent'],
    'contribute': ['CreateEvent', 'DeleteEvent', 'PullRequestEvent', 'PushEvent','ReleaseEvent', 'MemberEvent']
}

cat_list = ['contribute','interact','watch']


def define_category(event):
    for c in categories.keys():
        if event in categories[c]:
            return c


def processInput(submatrix, isDiag):
    edges = []
    if isDiag:
        for i in range(submatrix.shape[0]):
            for j in range(i, submatrix.shape[1]):
                w = submatrix[i][j]
                if w > 0:
                    edges.append(tuple((idx[i], idx[j], w)))
    else:
        for i in range(submatrix.shape[0]):
            for j in range(submatrix.shape[1]):
                w = submatrix[i][j]
                if w > 0:
                    edges.append(tuple((idx[i], idx[j], w)))
    return edges


path = '../data/'
a = pd.read_csv(path + 'activity_2016.csv', parse_dates=['timestamp'])
a['cat'] = a['event'].apply(lambda x: define_category(x))

print('Extract users collaborations')

u_projects = a[a['cat'] != 'watch'].groupby(by='source')['target'].apply(list)
u_projects = pd.DataFrame(u_projects)
u_projects['count'] = u_projects['target'].apply(lambda x: len(set(x)))
target_users = u_projects[u_projects['count'] > 1].index.values

t_a = a[a['source'].isin(target_users)]
u_multi = t_a.groupby(['source', 'target'])[['event']].count()
u_multi_all = u_multi.reset_index().pivot(index='source', columns='target', values='event').fillna(0)
u_multi_all['max'] = u_multi_all.apply(lambda x: max(x), axis=1)

# nodes
nodes = u_multi_all[u_multi_all['max'] > 100]['max'].reset_index()
nodes.columns = ['id', 'max_activities']
nodes.to_csv('graphs/nodes_multiproject.csv', index=None)

print('Nodes saved: {}'.format(nodes.shape[0]))

# edges: matrix multiplication to get similarity
M = u_multi_all[u_multi_all['max'] > 100].drop('max', axis=1)
E = M.dot(M.transpose(copy=True))

del E.index.name

idx = E.columns.values
E.values[[np.arange(len(E))]*2] = 0
E = E.values
N = E.shape[0]
n = N/6

print('Start parallel computation of edges')
start_t = time.time()

submatrices = []
start_row = 0
start_col = 0
end_row = n
end_col = n
for i in range(6):

    submatrices.append(E[start_row:end_row, start_col:end_col])
    
    start_col = end_col
    end_col = end_col + n
    
    if i == 2:
        start_row = end_row
        end_row = end_row + n
        start_col = n
        end_col = 2*n
        
    elif i == 4:
        start_row = end_row
        end_row = end_row + n
        start_col = 2*n
        end_col = 3*n
        
    # avoid outflow
    if end_col >= N:
        end_col = N
    if end_row >= N:
        end_row = N

results = Parallel(n_jobs=6)\
         (delayed(processInput)(m, is_diag) for m, is_diag in zip(submatrices,[True, False, False, True, False, True]))

print('End of parallel computation. It took {:.2f} min'.format((time.time()-start_t)/60))

edges = []
for r in results:
    edges = edges + r
edf = pd.DataFrame(edges, columns=['source', 'target', 'weight']).drop_duplicates()
edf.to_csv('graphs/edges_multiproject.csv', index=None)

print('Edges saved: {}'.format(edf.shape[0]))