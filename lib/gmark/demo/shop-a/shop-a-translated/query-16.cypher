MATCH (x0)-[:pauthor]->()-[:pfamilyName]->()<-[:ptext]-(x1), (x0)-[:pkeywords]->()<-[:pdescription]-()-[:pdescription]->()<-[:purl]-(x2), (x1)-[:pprintSection]->()<-[:phits]-(x3) RETURN DISTINCT x0, x1, x2, x3;
