WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s0.trg FROM edge s0 WHERE s0.label = 81)) , c1(src, trg) AS ((SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 81)) , c2(src, trg) AS ((SELECT s0.src, s0.trg FROM edge s0 WHERE s0.label = 72)) , c3(src, trg) AS ((SELECT edge.src, edge.src FROM edge UNION SELECT edge.trg, edge.trg FROM edge UNION SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 60)) , c4(src, trg) AS (SELECT src, trg FROM c3 UNION SELECT head.src, tail.trg FROM c3 as head, c4 as tail WHERE head.trg = tail.src) SELECT DISTINCT c0.src, c1.src, c2.src FROM c0, c1, c2, c3, c4 WHERE c3.src = c2.trg AND c2.src = c1.trg AND c1.src = c0.trg UNION ;
