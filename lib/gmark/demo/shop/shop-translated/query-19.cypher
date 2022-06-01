MATCH (x0)-[:p6]->(x1), (x1)<-[:p6]-(x2), (x0)-[:p6]->(x3), (x3)<-[:p6]-(x2) RETURN "true" LIMIT 1;
