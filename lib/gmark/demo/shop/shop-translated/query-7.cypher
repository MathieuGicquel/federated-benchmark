MATCH (x0)-[:p68]->()-[:p59]->()<-[:p59]-()<-[:p68]-(x1), (x1)-[:p68]->()-[:p57]->()<-[:p79]-(x2), (x0)-[:p70|p79*]->(x3), (x3)-[:p68|p69*]->(x2) RETURN DISTINCT x1, x2, x0, x3;
