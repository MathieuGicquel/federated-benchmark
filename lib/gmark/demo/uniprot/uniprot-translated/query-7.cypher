MATCH (x0)<-[:p6]-()<-[:p3]-()<-[:p0]-()-[:p1]->(x1), (x1)<-[:p1]-()-[:p2]->()<-[:p2]-()-[:p1]->(x2), (x2)<-[:p1]-()-[:p1]->()<-[:p1]-()-[:p3]->(x3), (x3)<-[:p3]-()<-[:p0]-()-[:p3]->()-[:p6]->(x4) RETURN DISTINCT x0;