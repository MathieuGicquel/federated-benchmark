WITH RECURSIVE c0(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = 72 AND s1.label = 71  UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, (SELECT trg as src, src as trg, label FROM edge) as s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 54 AND s1.label = 58  AND s2.label = 60  AND s3.label = 71  UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = 72 AND s1.label = 71 )) , c1(src, trg) AS (SELECT src, trg FROM c0 UNION SELECT head.src, tail.trg FROM c0 as head, c1 as tail WHERE head.trg = tail.src) , c2(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 81 AND s1.label = 81  AND s2.label = 81  AND s3.label = 81  UNION SELECT s0.src, s2.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 72 AND s1.label = 60  AND s2.label = 71  UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = 72 AND s1.label = 72 )) , c3(src, trg) AS (SELECT src, trg FROM c2 UNION SELECT head.src, tail.trg FROM c2 as head, c3 as tail WHERE head.trg = tail.src) , c4(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = 81 AND s1.label = 81  AND s2.label = 72  AND s3.label = 72  UNION SELECT s0.src, s2.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 71 AND s1.label = 58  AND s2.label = 54 )) , c5(src, trg) AS (SELECT src, trg FROM c4 UNION SELECT head.src, tail.trg FROM c4 as head, c5 as tail WHERE head.trg = tail.src) SELECT DISTINCT c0.src FROM c0, c1, c2, c3, c4, c5 WHERE c0.src = c2.src AND c4.src = c0.trg;
