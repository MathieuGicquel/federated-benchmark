MATCH (x0)<-[:p53]-()<-[:p76]-(x1), (x1)-[:p76]->()-[:p67]->()<-[:p67]-()-[:p81]->(x2), (x2)-[:p81*]->(x3) RETURN DISTINCT x0, x3;
