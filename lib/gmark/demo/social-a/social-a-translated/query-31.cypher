MATCH (x0)<-[:pbirthday]-()-[:pstudyAt]->()-[:pname]->()<-[:pcontent]-(x1), (x0)<-[:pcreationDate]-()-[:phasInterest]->()-[:pname]->()<-[:pname]-(x2), (x1)<-[:pbirthday]-()-[:pname]->()<-[:pname]-()-[:phasType]->(x3) RETURN DISTINCT x0, x1;