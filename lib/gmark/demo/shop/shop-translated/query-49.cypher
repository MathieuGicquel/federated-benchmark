MATCH (x0)<-[:p53]-()-[:p4]->()<-[:p34]-(x1), (x1)-[:p39]->()<-[:p7]-()<-[:p57]-()-[:p63]->(x2), (x0)<-[:p53]-()-[:p53]->()-[:p51]->(x3), (x3)<-[:p51]-()-[:p52]->()<-[:p52]-()-[:p53]->(x2) RETURN DISTINCT x0, x1, x2;
