MATCH (x0)-[:p55]->(x1), (x0)<-[:p61]-(x2), (x2)-[:p60*]->(x1) RETURN DISTINCT x1, x0, x2;
