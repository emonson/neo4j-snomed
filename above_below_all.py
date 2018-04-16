import py2neo
import pandas as pd
from collections import Counter

db = py2neo.Graph(password='neo.log')

df = pd.read_csv('uniqueIDsWlevels.csv', encoding='utf-8', dtype={'ID':'str', 'levels':'int'})
ids = df.ID.tolist()
n_ids = len(ids)

df['below'] = 0
df['above'] = 0

# Counting paths that lead to level 2 node
print('starting...')
for ii, id in enumerate(ids):
  # Below (number of IDs with shortest paths less than 12 steps leading up to current ID)
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: id}})-[:ISA*0..12]->(o2:ObjectConcept {{ sctid: '{id_val}' }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), id_val=id))
  df.loc[df['ID'] == id, 'below'] = nn[0]['cp']
  print('below', ii, ' of ', n_ids, '\t: ', nn[0]['cp'])

  # Above (number of IDs with shortest paths less than 12 that current ID leads up to)
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: '{id_val}'}})-[:ISA*0..12]->(o2:ObjectConcept {{ sctid: id }})) 
                    RETURN count(p) as cp;""".format(idslist=str(ids), id_val=id))
  df.loc[df['ID'] == id, 'above'] = nn[0]['cp']
  print('above', ii, ' of ', n_ids, '\t: ', nn[0]['cp'])

df.to_csv('uniqueIDsWlevels_belowAbove.csv', index=False, encoding='utf-8')
