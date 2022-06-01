MATCH (x0)-[:p60*]->(x1), (x1)-[:p60]->(x2), (x0)-[:p60*]->(x3), (x2)-[:p60*]->(x4) RETURN DISTINCT x1, x0, x2;
