MATCH (x0)<-[:p72]-()-[:p72]->()-[:p58]->()<-[:p54]-(x1), (x0)-[:p80|p60|p75*]->(x2), (x2)-[:p60]->()-[:p59]->()<-[:p71]-(x1) RETURN DISTINCT x0, x1;