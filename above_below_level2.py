import py2neo
import pandas as pd
from collections import Counter

db = py2neo.Graph(password='neo.log')

df = pd.read_csv('uniqueIDsWlevels.csv', encoding='utf-8', dtype={'ID':'str', 'levels':'int'})
ids = df.ID.tolist()
level2_df = df.loc[df['levels'] == 2]
level2_ids = level2_df.ID.tolist()
n_l2 = len(level2_ids)

df['below2'] = 0
df['above2'] = 0

# Counting paths that lead to level 2 node
print('starting...')
for ii,l2id in enumerate(level2_ids):
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: id}})-[:ISA*0..10]->(o2:ObjectConcept {{ sctid: '{l2val}' }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), l2val=l2id))
  df.loc[df['ID'] == l2id, 'below2'] = nn[0]['cp']
  print('below2', ii, ' of ', n_l2, '\t: ', nn[0]['cp'])

for ii,l2id in enumerate(level2_ids):
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: '{l2val}'}})-[:ISA*0..10]->(o2:ObjectConcept {{ sctid: id }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), l2val=l2id))
  df.loc[df['ID'] == l2id, 'above2'] = nn[0]['cp']
  print('above2', ii, ' of ', n_l2, '\t: ', nn[0]['cp'])

df.to_csv('uniqueIDsWlevels_belowAbove.csv', index=False, encoding='utf-8')
