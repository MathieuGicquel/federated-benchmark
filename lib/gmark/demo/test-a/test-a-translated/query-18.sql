WITH RECURSIVE c0(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = publishedIn AND s1.label = publishedIn  AND s2.label = publishedIn  AND s3.label = publishedIn  UNION SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = publishedIn AND s1.label = publishedIn  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = publishedIn AND s1.label = publishedIn  AND s2.label = publishedIn  AND s3.label = publishedIn )) , c1(src, trg) AS (SELECT src, trg FROM c0 UNION SELECT head.src, tail.trg FROM c0 as head, c1 as tail WHERE head.trg = tail.src) , c2(src, trg) AS ((SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = heldIn AND s1.label = heldIn  AND s2.label = heldIn  AND s3.label = heldIn )) , c3(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = publishedIn AND s1.label = publishedIn  AND s2.label = publishedIn  AND s3.label = publishedIn )) , c4(src, trg) AS (SELECT src, trg FROM c3 UNION SELECT head.src, tail.trg FROM c3 as head, c4 as tail WHERE head.trg = tail.src) SELECT DISTINCT c0.src, c0.trg  FROM c0, c1, c2, c3, c4 WHERE c3.src = c2.trg AND c0.trg = c3.trg AND c0.src = c2.src;
