MATCH (x0)<-[:pnationality]-()-[:plike]->()-[:pdatePublished]->(x1), (x0)<-[:pnationality]-()-[:pjobTitle]->()<-[:pdescription]-()-[:pexpires]->(x2), (x0)<-[:pnationality]-()<-[:pauthor]-()-[:pdatePublished]->(x3) RETURN DISTINCT x0;