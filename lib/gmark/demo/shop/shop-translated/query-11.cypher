MATCH (x0)<-[:p51]-()-[:p57]->(x1), (x1)-[:p81|p54|p79*]->(x2), (x2)-[:p54]->()<-[:p54]-()-[:p57]->(x3), (x3)-[:p65]->()-[:p49]->()<-[:p5]-()-[:p56]->(x4) RETURN DISTINCT x2, x0, x1;
