MATCH (x0)<-[:page]-()-[:pfollows]->()-[:puserId]->()<-[:pisbn]-(x1), (x1)-[:pauthor*]->(x2), (x0)<-[:page]-()-[:pgivenName]->()<-[:pkeywords]-(x3), (x3)<-[:plike]-()-[:pfriendOf]->()-[:pmakesPurchase]->()-[:pprice]->(x2) RETURN "true" LIMIT 1;