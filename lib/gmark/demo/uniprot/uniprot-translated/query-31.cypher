MATCH (x0)-[:p6]->()<-[:p6]-()<-[:p3]-()-[:p4]->(x1), (x1)<-[:p4]-()-[:p2]->(x2), (x0)<-[:p3]-()-[:p0]->()<-[:p0]-()<-[:p0]-(x3), (x3)-[:p0*]->(x2) RETURN "true" LIMIT 1;
