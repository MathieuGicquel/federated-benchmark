MATCH (x0)<-[:pheldIn]-()<-[:ppublishedIn]-()-[:pextendedTo]->(x1), (x0)<-[:pheldIn]-()<-[:ppublishedIn]-()-[:pextendedTo]->(x2), (x0)<-[:pheldIn]-()<-[:ppublishedIn]-()-[:pextendedTo]->(x3) RETURN "true" LIMIT 1;
