MATCH (x0)<-[:ptitle]-()-[:ptitle]->()<-[:pkeywords]-(x1), (x1)-[:ptext]->()<-[:pfamilyName]-()-[:pnationality]->(x2), (x0)<-[:paward]-()-[:pdirector]->()-[:plocation]->(x3), (x3)<-[:plocation]-()<-[:pauthor]-()-[:phasGenre]->()-[:ptype]->(x2) RETURN DISTINCT x0, x2;
