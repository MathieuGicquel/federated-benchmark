MATCH (x0)-[:p13]->(x1), (x1)<-[:p48]-(x2), (x0)-[:p53]->(x3), (x3)<-[:p53]-(x2) RETURN DISTINCT x2, x3, x1, x0;
