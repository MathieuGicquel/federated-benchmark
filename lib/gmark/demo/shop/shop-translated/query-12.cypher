MATCH (x0)<-[:p53]-()-[:p81]->(x1), (x0)<-[:p53]-()-[:p68]->()-[:p45]->()<-[:p14]-(x2), (x1)<-[:p53]-()-[:p24]->()<-[:p34]-()-[:p76]->(x3) RETURN DISTINCT x2, x3, x0, x1;