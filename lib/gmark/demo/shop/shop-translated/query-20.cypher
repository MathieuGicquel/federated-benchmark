MATCH (x0)-[:p79*]->(x1), (x0)-[:p78*]->(x2), (x0)-[:p79|p79*]->(x3), (x0)-[:p79|p79|p79*]->(x4) RETURN DISTINCT x3, x0, x2, x1;
