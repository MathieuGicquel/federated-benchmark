WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = OccursIn AND s1.label = HasKeyword  AND s2.label = HasKeyword  AND s3.label = EncodedOn )) , c1(src, trg) AS ((SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = EncodedOn AND s1.label = Reference  AND s2.label = Reference  AND s3.label = Reference  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = EncodedOn AND s1.label = OccursIn  AND s2.label = OccursIn  AND s3.label = Reference  UNION SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2, edge s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = EncodedOn AND s1.label = EncodedOn  AND s2.label = EncodedOn  AND s3.label = Reference )) , c2(src, trg) AS ((SELECT s0.src, s3.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = OccursIn AND s1.label = Reference  AND s2.label = AuthoredBy  AND s3.label = AuthoredBy )) , c3(src, trg) AS ((SELECT s0.src, s1.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1 WHERE s0.trg = s1.src AND s0.label = Reference AND s1.label = Reference  UNION SELECT s0.src, s2.trg FROM (SELECT trg as src, src as trg, label FROM edge) as s0, edge s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = Reference AND s1.label = Interacts  AND s2.label = Reference )) SELECT DISTINCT c0.src, c1.trg  FROM c0, c1, c2, c3 WHERE c3.src = c2.trg AND c1.trg = c3.trg AND c1.src = c0.trg AND c0.src = c2.src;
