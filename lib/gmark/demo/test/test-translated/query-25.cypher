MATCH (x0)<-[:p2]-()-[:p2]->(x1), (x0)<-[:p2]-()-[:p2]->(x2), (x1)<-[:p2]-()-[:p2]->(x3) RETURN DISTINCT x1, x2, x0;
