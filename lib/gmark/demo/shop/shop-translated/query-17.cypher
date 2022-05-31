MATCH (x0)-[:p52|p53*]->(x1), (x1)-[:p79*]->(x2), (x0)-[:p53*]->(x3), (x2)-[:p50*]->(x4) RETURN DISTINCT x0, x2 UNION ;
