MATCH (x0)-[:p81]->(x1), (x1)<-[:p81]-(x2), (x2)-[:p72]->(x3), (x3)-[:p60*]->(x4) RETURN DISTINCT x0, x1, x2 UNION ;
