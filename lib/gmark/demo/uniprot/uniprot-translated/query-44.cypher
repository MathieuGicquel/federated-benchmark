MATCH (x0)<-[:p5]-()<-[:p3]-()-[:p2]->()<-[:p2]-(x1), (x1)-[:p0|p0|p0*]->(x2), (x2)-[:p0]->()-[:p4]->(x3), (x3)<-[:p4]-()<-[:p0]-()-[:p2]->(x4) RETURN DISTINCT x0, x4;