MATCH (x0)-[:p54|p54*]->(x1), (x1)-[:p54|p54|p54*]->(x2), (x0)-[:p54|p42|p54*]->(x3), (x2)-[:p42|p58|p41*]->(x4) RETURN DISTINCT x3, x2, x1, x0;
