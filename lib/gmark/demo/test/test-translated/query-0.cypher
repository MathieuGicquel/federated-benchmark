MATCH (x0)-[:p1*]->(x1), (x0)-[:p1*]->(x2), (x0)-[:p1|p1*]->(x3), (x0)-[:p1|p1|p1*]->(x4) RETURN DISTINCT x0, x1;
