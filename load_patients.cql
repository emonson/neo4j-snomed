// Diagnosed with

	// Uniqueness constraints.
CREATE CONSTRAINT ON (p:Patient) ASSERT p.id IS UNIQUE;

	// Load.
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS 
FROM "file:///Users/emonson/Dropbox/People/AskData/MarciBowen/neo4j-snomed/data/UCD_Dataset_SCT_by_Pt.csv" AS line
WITH line
WHERE line.participant_id IS NOT NULL AND
      line.SNOMEDConceptID IS NOT NULL

MATCH (code:ObjectConcept { sctid: line.SNOMEDConceptID })

MERGE (patient:Patient { id: line.participant_id })

CREATE (patient)-[:HAS_DX]->(code)
;
