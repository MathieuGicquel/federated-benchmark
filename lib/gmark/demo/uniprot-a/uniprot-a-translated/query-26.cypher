MATCH (x0)<-[:pOccursIn]-()-[:pInteracts]->()-[:pOccursIn]->(x1), (x0)<-[:pOccursIn]-()<-[:pInteracts]-()-[:pReference]->()-[:pPublishedIn]->(x2), (x2)<-[:pPublishedIn]-()<-[:pReference]-()-[:pHasKeyword]->(x1) RETURN DISTINCT x0;