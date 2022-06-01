MATCH (x0)<-[:p52]-(x1), (x1)<-[:p53]-(x2), (x2)-[:p53]->(x3), (x3)<-[:p53]-(x4) RETURN DISTINCT x2, x1, x0;
