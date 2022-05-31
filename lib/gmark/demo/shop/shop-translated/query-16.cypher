MATCH (x0)<-[:p61]-()<-[:p71]-()-[:p81]->(x1), (x1)-[:p81|p80|p81*]->(x2), (x2)-[:p80*]->(x3) RETURN DISTINCT x0;
