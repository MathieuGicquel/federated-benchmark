MATCH (x0)<-[:pheldIn]-()-[:pheldIn]->(x1), (x0)<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->(x2), (x0)<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->(x3), (x0)<-[:pheldIn]-()<-[:ppublishedIn]-()-[:ppublishedIn]->()-[:pheldIn]->(x4) RETURN DISTINCT x3, x1, x2, x0;
