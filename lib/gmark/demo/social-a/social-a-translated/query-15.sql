WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = email AND s1.label = speaks  AND s2.label = name  AND s3.label = studyAt )) , c1(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = locationIP AND s1.label = isLocatedIn  AND s2.label = isLocatedIn )) , c2(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = speaks AND s1.label = email  AND s2.label = name )) SELECT DISTINCT c2.trg , c0.src, c1.trg , c0.trg  FROM c0, c1, c2 WHERE c0.src = c1.src AND c0.src = c2.src;