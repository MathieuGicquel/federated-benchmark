MATCH (x0)<-[:p0]-()-[:p0]->()-[:p1]->()<-[:p1]-(x1), (x0)<-[:p0]-()-[:p0]->(x2), (x0)<-[:p0]-()-[:p0]->()-[:p1]->()<-[:p1]-(x3), (x0)-[:p1]->()<-[:p1]-()-[:p3]->()<-[:p3]-(x4) RETURN "true" LIMIT 1;
