MATCH (x0)-[:pbrowserUsed]->()<-[:pgender]-()-[:pworksAt]->(x1), (x1)-[:pname|pname*]->(x2), (x0)<-[:pisSubclassOf]-()-[:planguage]->()<-[:pname]-(x3), (x3)-[:pname|pname|pname*]->(x2) RETURN DISTINCT x0;