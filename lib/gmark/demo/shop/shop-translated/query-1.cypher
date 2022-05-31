MATCH (x0)-[:p60*]->(x1), (x0)-[:p75|p60*]->(x2), (x1)-[:p60*]->(x3) RETURN DISTINCT x0;
