MATCH (x0)<-[:p53]-(x1), (x1)-[:p72]->(x2), (x2)-[:p60*]->(x3), (x3)-[:p48]->(x4) RETURN DISTINCT x3, x1, x2, x0;
