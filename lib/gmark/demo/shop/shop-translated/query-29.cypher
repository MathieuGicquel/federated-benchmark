MATCH (x0)<-[:p79]-()-[:p79]->()-[:p72]->()-[:p57]->(x1), (x1)-[:p71]->()-[:p59]->()<-[:p67]-()<-[:p76]-(x2), (x0)-[:p81*]->(x3), (x3)-[:p54|p54|p79*]->(x2) RETURN DISTINCT x1, x0, x2;
