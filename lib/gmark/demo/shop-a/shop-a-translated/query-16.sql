WITH RECURSIVE c0(src, trg) AS ((SELECT s0.src, s2.trg FROM edge s0, edge s1, (SELECT trg as src, src as trg, label FROM edge) as s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = author AND s1.label = familyName  AND s2.label = text  UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = description AND s1.label = keywords )) , c1(src, trg) AS ((SELECT s0.src, s3.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2, (SELECT trg as src, src as trg, label FROM edge) as s3 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s2.trg = s3.src AND s0.label = keywords AND s1.label = description  AND s2.label = description  AND s3.label = url  UNION SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = contentSize AND s1.label = hits  UNION SELECT s0.src, s2.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1, edge s2 WHERE s0.trg = s1.src AND s1.trg = s2.src AND s0.label = wordCount AND s1.label = printPage  AND s2.label = homepage )) , c2(src, trg) AS ((SELECT s0.src, s1.trg FROM edge s0, (SELECT trg as src, src as trg, label FROM edge) as s1 WHERE s0.trg = s1.src AND s0.label = printSection AND s1.label = hits )) SELECT DISTINCT c0.src, c2.src, c1.trg , c2.trg  FROM c0, c1, c2 WHERE c2.src = c0.trg AND c0.src = c1.src;