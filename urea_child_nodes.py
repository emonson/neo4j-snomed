import py2neo
import pandas as pd
from numpy import int64
from collections import Counter

db = py2neo.Graph(password='neo.log')

# Actual Urea Cycle data codes which will be tested as possible children of high-level codes
df = pd.read_csv('urea_cycle_data.csv', encoding='utf-8', dtype={'Source':'str', 'SourceName':'str', 'Number of Patient':'int'})
ids = df.Source.tolist()
n_ids = len(ids)

# High-level codes which will be tested for how many data codes they are parents of
# ID	level	below	above	FSN	Name	Module
parent_df = pd.read_csv('urea_parent_nodes.csv', encoding='utf-8', dtype='str')
parent_ids = parent_df.ID.tolist()
n_parents = len(parent_ids)

# Force integer type of the count because was putting decimal places in file...
parent_df['n_matches'] = int64(0)

# Paths leading to high-level (coarse) nodes
print('starting...')
for ii, id in enumerate(parent_ids):
  # Below (number of IDs with shortest paths less than 12 steps leading up to current ID)
  nn = db.data("""WITH {idslist} AS ids 
                    UNWIND ids AS id 
                    MATCH p=shortestpath((o1:ObjectConcept {{ sctid: id}})-[:ISA*0..12]->(o2:ObjectConcept {{ sctid: '{id_val}' }})) 
                    RETURN o2.sctid as parent, collect(id) as children;""".format(idslist=str(ids), id_val=id))
  if len(nn) > 0:
    parent_node = nn[0]['parent']
    child_nodes = nn[0]['children']
    print(ii, '/', n_parents, ' : ', parent_node, ' : ', len(child_nodes))
    parent_df.loc[parent_df['ID'] == id, 'n_matches'] = len(child_nodes)
    parent_df.loc[parent_df['ID'] == id, 'match_ids'] = '|'.join(child_nodes)
  else:
    print(ii, '/', n_parents, ' : ', parent_node, ' : 0')

parent_df.to_csv('urea_child_nodes.csv', index=False, encoding='utf-8')
