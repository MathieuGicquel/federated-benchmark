MATCH (x0)-[:p16|p16*]->(x1), (x1)-[:p16]->()<-[:p22]-()-[:p19]->(x2), (x2)<-[:p24]-()-[:p13]->()<-[:p13]-(x3) RETURN "true" LIMIT 1;