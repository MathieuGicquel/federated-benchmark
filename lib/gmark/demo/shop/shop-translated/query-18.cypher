MATCH (x0)-[:p70|p76*]->(x1), (x1)-[:p68]->()-[:p57]->()<-[:p57]-(x2), (x0)-[:p11]->()<-[:p0]-()-[:p0]->()<-[:p5]-(x3), (x3)-[:p54]->()<-[:p58]-()-[:p61]->(x2) RETURN DISTINCT x2, x0, x1, x3;
