MATCH (x0)-[:ppurchaseFor]->()-[:pproducer]->()<-[:pkeywords]-()-[:phasReview]->(x1), (x1)-[:previewer|previewer*]->(x2), (x2)-[:previewer]->()-[:plike]->()<-[:plike]-()-[:phomepage]->(x3) RETURN "true" LIMIT 1;
