MATCH (x0)<-[:pReference]-()<-[:pInteracts]-()-[:pReference]->()-[:pAuthoredBy]->(x1), (x1)<-[:pAuthoredBy]-()<-[:pReference]-()-[:pHasKeyword]->()<-[:pHasKeyword]-(x2), (x0)-[:pPublishedIn]->()<-[:pPublishedIn]-()-[:pPublishedIn]->(x3), (x3)<-[:pPublishedIn]-()-[:pAuthoredBy]->()<-[:pAuthoredBy]-()-[:pAuthoredBy]->(x2) RETURN DISTINCT x0;