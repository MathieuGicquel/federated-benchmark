MATCH (x0)<-[:p20]-()-[:p19]->()<-[:p16]-(x1), (x0)<-[:p19]-()-[:p19]->()<-[:p16]-(x2), (x0)<-[:p20]-()-[:p19]->()<-[:p16]-(x3), (x0)<-[:p20]-()<-[:p0]-()-[:p17]->()<-[:p16]-(x4) RETURN DISTINCT x2, x1, x0;