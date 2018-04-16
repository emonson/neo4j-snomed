import py2neo
import pandas as pd
from collections import Counter

db = py2neo.Graph(password='neo.log')

df = pd.read_csv('optFiltStepwise_uniqueIDs.csv', encoding='utf-8', dtype='str')
ids = df.ID.tolist()

# Shortest paths to root from each start node, counting steps
levels = db.data("""WITH {idslist} AS ids 
                  UNWIND ids AS id 
                  MATCH p=shortestpath((o1:ObjectConcept {{ sctid: id}})-[:ISA*0..30]->(o2:ObjectConcept {{ sctid: '138875005' }})) 
                  RETURN length(p) AS l;""".format(idslist=str(ids)) )
ll = [x['l'] for x in levels]
counts = Counter(ll)
print(sorted(counts.items(), key=lambda x: x[0]))

df['levels'] = ll
# df.to_csv('uniqueIDsWlevels.csv', index=False, encoding='utf-8')
