MATCH (x0)-[:p71*]->(x1), (x1)-[:p54*]->(x2), (x0)-[:p71]->()-[:p57]->()<-[:p57]-()-[:p78]->(x3), (x3)-[:p79|p79|p78*]->(x2) RETURN DISTINCT x2, x0, x1;
