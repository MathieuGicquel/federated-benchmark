MATCH (x0)-[:p41]->(x1), (x1)<-[:p50]-(x2), (x0)-[:p42]->(x3), (x3)<-[:p34]-(x2) RETURN DISTINCT x3, x0, x1, x2;
