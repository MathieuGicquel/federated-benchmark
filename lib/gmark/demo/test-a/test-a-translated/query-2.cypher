MATCH (x0)<-[:pheldIn]-()<-[:ppublishedIn]-()-[:ppublishedIn]->()<-[:pheldIn]-()-[:pheldIn]->(x1), (x0)<-[:ppublishedIn]-()-[:ppublishedIn]->()<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->(x2), (x1)<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->()<-[:pheldIn]-()-[:pheldIn]->(x3) RETURN DISTINCT x0 UNION ;
