WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Reference AND s1.label = Reference  AND s2.label = Reference )) , c1(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s1.trg FROM edge s0, edge s1 WHERE s0.trg = s1.src AND s0.label = Interacts AND s1.label = Interacts  UNION SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = Interacts AND s1.label = Interacts  AND s2.label = Interacts  AND s3.label = Interacts  UNION SELECT s0.src, s2.trg FROM edge s0, edge s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Interacts AND s1.label = Interacts  AND s2.label = Interacts )) , c2(src, trg) AS (SELECT src, trg FROM c1 UNION SELECT head.src, tail.trg FROM c1 as head, c2 as tail WHERE head.trg = tail.src) , c3(src, trg) AS ((SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Reference AND s1.label = Interacts  AND s2.label = EncodedOn )) , c4(src, trg) AS ((SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = EncodedOn AND s1.label = EncodedOn )) SELECT DISTINCT c1.trg , c1.src, c0.src FROM c0, c1, c2, c3, c4 WHERE c4.src = c3.trg AND c1.trg = c4.trg AND c1.src = c0.trg AND c0.src = c3.src;
