MATCH (x0)-[:p65|p65*]->(x1), (x0)-[:p65|p65*]->(x2), (x2)-[:p79*]->(x1) RETURN DISTINCT x0, x1, x2;
