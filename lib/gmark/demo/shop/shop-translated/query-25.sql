WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 56 AND s1.label = 54  AND s2.label = 6  UNION SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = 56 AND s1.label = 6 )) , c1(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = 56 AND s1.label = 14  AND s2.label = 1 )) , c2(src, trg) AS ((SELECT s0.src, s1.trg FROM edge s0, edge s1 WHERE s0.trg = s1.src AND s0.label = 54 AND s1.label = 56 )) SELECT DISTINCT c0.src FROM c0, c1, c2 WHERE c2.src = c1.trg AND c0.src = c1.src AND c0.trg = c2.trg;
