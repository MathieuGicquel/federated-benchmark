MATCH (x0)<-[:pHasKeyword]-()-[:pReference]->()-[:pPublishedIn]->(x1), (x1)<-[:pPublishedIn]-()<-[:pReference]-()-[:pOccursIn]->(x2), (x0)<-[:pHasKeyword]-()<-[:pInteracts]-()<-[:pInteracts]-()-[:pHasKeyword]->(x3), (x2)<-[:pHasKeyword]-()<-[:pInteracts]-()-[:pHasKeyword]->(x4) RETURN DISTINCT x1, x0, x2, x3;