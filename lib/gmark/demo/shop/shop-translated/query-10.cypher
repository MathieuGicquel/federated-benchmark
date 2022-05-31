MATCH (x0)-[:p76|p71*]->(x1), (x1)-[:p54|p79*]->(x2), (x0)-[:p71*]->(x3), (x3)-[:p72|p71|p72*]->(x2) RETURN DISTINCT x2, x3, x0, x1;
