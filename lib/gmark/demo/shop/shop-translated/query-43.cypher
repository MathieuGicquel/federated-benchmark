MATCH (x0)-[:p60*]->(x1), (x0)<-[:p80]-(x2), (x2)<-[:p81]-(x1) RETURN "true" LIMIT 1;
