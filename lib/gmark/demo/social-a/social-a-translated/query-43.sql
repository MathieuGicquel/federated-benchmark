WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s3.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = isSubclassOf AND s1.label = name  AND s2.label = language  AND s3.label = content  UNION SELECT s0.src, s3.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = isSubclassOf AND s1.label = name  AND s2.label = name  AND s3.label = name )) , c1(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = length AND s1.label = length  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = content AND s1.label = content  AND s2.label = name  AND s3.label = locationIP )) , c2(src, trg) AS (SELECT src, trg FROM c1 UNION SELECT head.src, tail.trg FROM c1 as head, c2 as tail WHERE head.trg = tail.src) , c3(src, trg) AS ((SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = hasType AND s1.label = hasType )) , c4(src, trg) AS ((SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = isSubclassOf AND s1.label = name  AND s2.label = locationIP  AND s3.label = creationDate  UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = name AND s1.label = locationIP  AND s2.label = hasModerator  AND s3.label = creationDate  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = isSubclassOf AND s1.label = name  AND s2.label = locationIP  AND s3.label = creationDate )) SELECT DISTINCT c1.trg , c0.src, c1.src FROM c0, c1, c2, c3, c4 WHERE c4.src = c3.trg AND c1.trg = c4.trg AND c1.src = c0.trg AND c0.src = c3.src;