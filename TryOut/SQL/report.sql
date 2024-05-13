SELECT _rowid, actual_price, MAX(_version)
FROM _tblHistoryMaterials
WHERE
    _rowid IS NOT NULL
    AND actual_price IS NOT NULL
    AND _rowid <> 0
GROUP BY _rowid

WITH vars(hmax) AS (
            SELECT COALESCE(MAX(hm._version), 0) hmax
            FROM _tblHistoryMaterials hm
            WHERE
                hm._rowid IS NOT NULL
                AND hm.actual_price IS NOT NULL
            GROUP BY hm._rowid
            LIMIT 1
        )
select m.*
FROM _tblHistoryMaterials h
JOIN vars ON vars.hmax = h._version
;


SELECT last_price, hm.*
FROM _tblHistoryMaterials hm
JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
JOIN (
    SELECT hm2._rowid, MAX(p2.index_num) last_index, MAX(hm2.actual_price) last_price
    FROM _tblHistoryMaterials hm2
    JOIN tblPeriods p2 ON p2.ID_tblPeriod = hm2.FK_tblMaterials_tblPeriods
    WHERE hm2._rowid = 3
    GROUP BY hm2._rowid
) AS subquery
ON subquery._rowid = hm._rowid
WHERE hm._rowid = 3 AND p.index_num <= subquery.last_index;


SELECT p.index_num, sub_actual_price.last_actual_price, sub_base_price.last_base_price, last_estimate_price, hm.*
FROM _tblHistoryMaterials hm
JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
JOIN (
    SELECT hm2._rowid, MAX(p2.index_num) last_index, hm2.actual_price last_actual_price
    FROM _tblHistoryMaterials hm2
    JOIN tblPeriods p2 ON p2.ID_tblPeriod = hm2.FK_tblMaterials_tblPeriods
    WHERE hm2.actual_price IS NOT NULL
    GROUP BY hm2._rowid
    ) AS sub_actual_price
    ON sub_actual_price._rowid = hm._rowid
JOIN (
    SELECT hm3._rowid, MAX(p3.index_num) last_index, hm3.base_price last_base_price
    FROM _tblHistoryMaterials hm3
    JOIN tblPeriods p3 ON p3.ID_tblPeriod = hm3.FK_tblMaterials_tblPeriods
    WHERE hm3.base_price IS NOT NULL
    GROUP BY hm3._rowid
    ) AS sub_base_price
    ON sub_base_price._rowid = hm._rowid
JOIN (
    SELECT hm3._rowid, MAX(p3.index_num) last_index, hm3.estimate_price last_estimate_price
    FROM _tblHistoryMaterials hm3
    JOIN tblPeriods p3 ON p3.ID_tblPeriod = hm3.FK_tblMaterials_tblPeriods
    WHERE hm3.estimate_price IS NOT NULL
    GROUP BY hm3._rowid
    ) AS sub_estimate_price
    ON sub_estimate_price._rowid = hm._rowid
WHERE hm._rowid = 3
;


SELECT
    hm._rowid,
    p.index_num,
    sub_actual_price.last_actual_price,
    sub_base_price.last_base_price,
    last_estimate_price
    , hm.*
FROM _tblHistoryMaterials hm
JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
JOIN (
    SELECT hm2._rowid, MAX(p2.index_num) last_index, hm2.actual_price last_actual_price
    FROM _tblHistoryMaterials hm2
    JOIN tblPeriods p2 ON p2.ID_tblPeriod = hm2.FK_tblMaterials_tblPeriods
    WHERE hm2.actual_price IS NOT NULL  AND p2.index_num <= 205
    GROUP BY hm2._rowid
    ) AS sub_actual_price
    ON sub_actual_price._rowid = hm._rowid
JOIN (
    SELECT hm3._rowid, MAX(p3.index_num) last_index, hm3.base_price last_base_price
    FROM _tblHistoryMaterials hm3
    JOIN tblPeriods p3 ON p3.ID_tblPeriod = hm3.FK_tblMaterials_tblPeriods
    WHERE hm3.base_price IS NOT NULL AND p3.index_num <= 205
    GROUP BY hm3._rowid
    ) AS sub_base_price
    ON sub_base_price._rowid = hm._rowid
JOIN (
    SELECT hm4._rowid, MAX(p4.index_num) last_index, hm4.estimate_price last_estimate_price
    FROM _tblHistoryMaterials hm4
    JOIN tblPeriods p4 ON p4.ID_tblPeriod = hm4.FK_tblMaterials_tblPeriods
    WHERE hm4.estimate_price IS NOT NULL AND p4.index_num <= 205
    GROUP BY hm4._rowid
    ) AS sub_estimate_price
    ON sub_estimate_price._rowid = hm._rowid
WHERE hm._rowid = 3
    AND p.index_num = 205
;
