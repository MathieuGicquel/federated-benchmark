MATCH (x0)-[:p72|p76*]->(x1), (x1)-[:p81|p72|p71*]->(x2), (x2)-[:p52]->()<-[:p52]-()-[:p51]->(x3), (x3)<-[:p51]-()<-[:p53]-()<-[:p79]-(x4) RETURN DISTINCT x0;
