MATCH (x0)<-[:p53]-(x1), (x1)-[:p67]->(x2), (x2)-[:p60*]->(x3), (x3)-[:p51]->(x4) RETURN DISTINCT x0;
