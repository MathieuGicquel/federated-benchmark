MATCH (x0)-[:pincludes]->()-[:pperformer]->()<-[:pdescription]-(x1), (x1)-[:pconductor|phomepage|phasReview*]->(x2), (x0)-[:pincludes*]->(x3), (x3)-[:pincludes|pincludes|pincludes*]->(x2) RETURN DISTINCT x0;