WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = EncodedOn AND s1.label = EncodedOn  AND s2.label = Interacts  AND s3.label = EncodedOn  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = Interacts AND s1.label = HasKeyword  AND s2.label = HasKeyword  AND s3.label = EncodedOn  UNION SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Reference AND s1.label = Reference  AND s2.label = EncodedOn )) , c1(src, trg) AS ((SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = HasKeyword AND s1.label = HasKeyword  AND s2.label = EncodedOn )) , c2(src, trg) AS ((SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Reference AND s1.label = Reference  AND s2.label = EncodedOn  UNION SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = EncodedOn AND s1.label = EncodedOn  AND s2.label = EncodedOn  UNION SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = EncodedOn AND s1.label = EncodedOn  AND s2.label = EncodedOn )) SELECT "true" FROM edge WHERE EXISTS (SELECT * FROM c0, c1, c2 WHERE c0.src = c1.src AND c0.src = c2.src);
