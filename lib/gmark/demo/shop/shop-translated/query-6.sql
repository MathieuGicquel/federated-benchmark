WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 65)) , c1(src, trg) AS ((SELECT s0.src, s0.trg FROM edge s0 WHERE s0.label = 66)) , c2(src, trg) AS ((SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 61)) SELECT DISTINCT c0.src, c2.src, c1.src, c2.trg  FROM c0, c1, c2 WHERE c2.src = c1.trg AND c1.src = c0.trg;
