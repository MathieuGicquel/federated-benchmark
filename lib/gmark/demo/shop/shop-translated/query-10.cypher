MATCH (x0)<-[:p52]-(x1), (x0)<-[:p52]-(x2), (x2)-[:p5]->(x1) RETURN DISTINCT x2, x1, x0;
