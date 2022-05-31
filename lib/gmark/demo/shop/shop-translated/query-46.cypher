MATCH (x0)-[:p80|p81*]->(x1), (x0)-[:p81|p81|p81*]->(x2), (x0)-[:p81*]->(x3) RETURN DISTINCT x0;
