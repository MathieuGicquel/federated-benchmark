MATCH (x0)<-[:pOccursIn]-()-[:pHasKeyword]->()<-[:pHasKeyword]-()-[:pInteracts]->(x1), (x1)-[:pInteracts|pInteracts*]->(x2), (x0)<-[:pOccursIn]-()-[:pReference]->()-[:pAuthoredBy]->(x3), (x3)<-[:pAuthoredBy]-()-[:pPublishedIn]->()<-[:pPublishedIn]-()-[:pPublishedIn]->(x2) RETURN DISTINCT x3, x2, x0, x1;