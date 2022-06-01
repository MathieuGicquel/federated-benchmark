WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s0.trg FROM edge s0 WHERE s0.label = 23)) , c1(src, trg) AS ((SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 35)) , c2(src, trg) AS ((SELECT s0.src, s0.trg FROM edge s0 WHERE s0.label = 25)) , c3(src, trg) AS ((SELECT s0.src, s0.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0 WHERE s0.label = 32)) SELECT DISTINCT c1.trg , c0.src, c1.src, c3.src FROM c0, c1, c2, c3 WHERE c3.src = c2.trg AND c1.trg = c3.trg AND c0.src = c2.src AND c1.src = c0.trg;
