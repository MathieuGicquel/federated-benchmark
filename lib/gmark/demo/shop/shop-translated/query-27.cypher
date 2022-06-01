MATCH (x0)-[:p42]->()<-[:p8]-(x1), (x1)<-[:p35]-()-[:p6]->(x2), (x0)<-[:p14]-(x3), (x3)-[:p6]->(x2) RETURN DISTINCT x3, x0, x1, x2 UNION ;
