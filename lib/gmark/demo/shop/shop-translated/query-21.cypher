MATCH (x0)-[:p3]->(x1), (x1)<-[:p5]-(x2), (x0)-[:p52]->(x3), (x3)<-[:p52]-(x2) RETURN DISTINCT x0, x2;
