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
