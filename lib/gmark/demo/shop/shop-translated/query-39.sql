WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 53 AND s1.label = 3  AND s2.label = 29 )) , c1(src, trg) AS ((SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 57 AND s1.label = 57  AND s2.label = 80  UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, (SELECT trg as src, src as trg, label FROM edge) as s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 57 AND s1.label = 57  AND s2.label = 59  AND s3.label = 80  UNION SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 59 AND s1.label = 67  AND s2.label = 81 )) , c2(src, trg) AS ((SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 80 AND s1.label = 60  AND s2.label = 57  UNION SELECT s0.src, s2.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 80 AND s1.label = 59  AND s2.label = 71  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, edge s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 81 AND s1.label = 69  AND s2.label = 60  AND s3.label = 57 )) , c3(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, (SELECT trg as src, src as trg, label FROM edge) as s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 71 AND s1.label = 60  AND s2.label = 60  AND s3.label = 71  UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = 81 AND s1.label = 81 )) , c4(src, trg) AS (SELECT src, trg FROM c3 UNION SELECT head.src, tail.trg FROM c3 as head, c4 as tail WHERE head.trg = tail.src) SELECT DISTINCT c2.src, c3.src, c0.src, c1.src FROM c0, c1, c2, c3, c4 WHERE c3.src = c2.trg AND c2.src = c1.trg AND c1.src = c0.trg;
