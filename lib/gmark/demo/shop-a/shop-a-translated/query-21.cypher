MATCH (x0)<-[:ptype]-()<-[:phasGenre]-()-[:pwordCount]->(x1), (x0)<-[:ptype]-()<-[:phasGenre]-()-[:pcontentRating]->(x2), (x0)<-[:ptype]-()<-[:phasGenre]-()-[:pcontentRating]->(x3), (x0)<-[:ptype]-()-[:ptag]->()<-[:ptag]-()-[:pcontentSize]->(x4) RETURN DISTINCT x0, x1;
