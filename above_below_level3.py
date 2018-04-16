import py2neo
import pandas as pd
from collections import Counter

db = py2neo.Graph(password='neo.log')

df = pd.read_csv('uniqueIDsWlevels_belowAbove.csv', encoding='utf-8', dtype={'ID':'str', 'levels':'int'})
ids = df.ID.tolist()
level3_df = df.loc[df['levels'] == 3]
level3_ids = level3_df.ID.tolist()
n_l3 = len(level3_ids)

df['below3'] = 0
df['above3'] = 0

# Counting paths that lead to level 2 node
print('starting...')
for ii,l3id in enumerate(level3_ids):
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: id}})-[:ISA*0..10]->(o2:ObjectConcept {{ sctid: '{l3val}' }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), l3val=l3id))
  df.loc[df['ID'] == l3id, 'below3'] = nn[0]['cp']
  print('below3', ii, ' of ', n_l3, '\t: ', nn[0]['cp'])

for ii,l3id in enumerate(level3_ids):
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: '{l3val}'}})-[:ISA*0..10]->(o2:ObjectConcept {{ sctid: id }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), l3val=l3id))
  df.loc[df['ID'] == l3id, 'above3'] = nn[0]['cp']
  print('above3', ii, ' of ', n_l3, '\t: ', nn[0]['cp'])

df.to_csv('uniqueIDsWlevels_belowAbovel3.csv', index=False, encoding='utf-8')
