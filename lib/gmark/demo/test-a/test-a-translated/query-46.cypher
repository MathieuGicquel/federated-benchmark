MATCH (x0)-[:ppublishedIn|ppublishedIn*]->(x1), (x0)-[:ppublishedIn|ppublishedIn*]->(x2), (x0)-[:ppublishedIn|ppublishedIn*]->(x3) RETURN DISTINCT x2, x1, x3, x0;
