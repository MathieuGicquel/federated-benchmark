MATCH (x0)-[:p23]->(x1), (x1)<-[:p35]-(x2), (x0)-[:p25]->(x3), (x3)<-[:p32]-(x2) RETURN DISTINCT x2, x0, x1, x3;
