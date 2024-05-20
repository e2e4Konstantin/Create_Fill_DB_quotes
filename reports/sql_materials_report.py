sql_materials_reports = {
    # 1
    "select_ton_supplement_for_index": """--sql
        -- получить номер дополнения для индекса в справочнике ТСН
        SELECT p.supplement_num AS supplement_number
        FROM tblPeriods p
        JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
        JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
        WHERE
            o.name = 'ТСН'
            AND i.team = 'periods_category'
            AND i.name = 'index'
            AND p.index_num = :ton_index_number
        ;
    """,
    # 2
    "select_products_from_history": """--sql
        /*
        Из истории продуктов Материалы для дополнения :supplement.
        Только не удаленные записи. Вытягиваем последнее изменение полей.
        Кроме 0-й главы.
        */
        WITH
            -- FK_tblProducts_tblOrigins
            latest_origins AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_origins, hp.FK_tblProducts_tblOrigins
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.FK_tblProducts_tblOrigins IS NOT NULL and p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- FK_tblProducts_tblItems
            latest_item AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_item, hp.FK_tblProducts_tblItems
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.FK_tblProducts_tblItems IS NOT NULL AND p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- code
            latest_code AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_code, hp.code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- ID_tblProduct
            latest_ID_tblProduct AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_ID_tblProduct, hp.ID_tblProduct
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.ID_tblProduct IS NOT NULL AND p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- description
            latest_description AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_description, hp.description
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.description IS NOT NULL AND p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- measurer
            latest_measurer AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_measurer, hp.measurer
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.measurer IS NOT NULL and p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- digit_code
            latest_digit_code AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_digit_code, hp.digit_code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.digit_code IS NOT NULL and p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- FK_tblProducts_tblCatalogs
            latest_catalog AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_catalogs, hp.FK_tblProducts_tblCatalogs
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.FK_tblProducts_tblCatalogs IS NOT NULL and p.supplement_num <= :supplement
                GROUP BY hp._rowid
            ),
            -- period
            target_periods AS (
                SELECT p.ID_tblPeriod, p.supplement_num, p.title
                FROM tblPeriods p
                JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                WHERE
                    p.supplement_num = :supplement
                    AND o.name  = 'ТСН'
                    AND i.team = 'periods_category'
                    AND i.name = 'supplement'
            )
        SELECT
            hp._mask AS material_mask,
            hp._rowid AS material_rowid,
            lid.ID_tblProduct AS material_product_id,
            lcode.code AS material_code,
            ldesc.description AS material_description,
            lmeas.measurer AS material_measurer,
            ldc.digit_code AS material_digit_code,
            lcatalog.FK_tblProducts_tblCatalogs AS material_catalog_id,
            cat.code AS material_catalog_code
        FROM _tblHistoryProducts hp
        JOIN target_periods tp ON tp.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
        JOIN latest_origins lorigin ON lorigin._rowid = hp._rowid
        JOIN latest_item litem ON litem._rowid = hp._rowid
        --
        JOIN latest_ID_tblProduct lid ON lid._rowid = hp._rowid
        JOIN latest_code lcode ON lcode._rowid = hp._rowid
        JOIN latest_description ldesc ON ldesc._rowid = hp._rowid
        JOIN latest_measurer lmeas ON lmeas._rowid = hp._rowid
        JOIN latest_digit_code ldc ON ldc._rowid = hp._rowid
        JOIN latest_catalog lcatalog ON lcatalog._rowid = hp._rowid
        --
        JOIN tblOrigins o ON o.ID_tblOrigin = lorigin.FK_tblProducts_tblOrigins
        JOIN tblItems i ON i.ID_tblItem = litem.FK_tblProducts_tblItems
        JOIN tblCatalogs cat ON cat.ID_tblCatalog = lcatalog.FK_tblProducts_tblCatalogs
        WHERE
            hp._mask != -1
            AND o.name = 'ТСН'
            AND i.team = 'units'
            AND i.name = 'material'
            AND cat.code != '1.0-0'
        ORDER BY ldc.digit_code
        ;
        """,
    # --- 3 ---
    "select_history_material_for_period": """--sql
    /*
        по номеру индекса периода выбирает материалы из таблицы истории
        с последними изменениями до указанного периода.
        Удаленные материалы не учитываются.
        {'index_number': 207, 'product_id': 6}
    */
        WITH
        -- ID_tblMaterial
        latest_ID_tblMaterial AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.ID_tblMaterial
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.ID_tblMaterial IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- actual_price
        latest_actual_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.actual_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.actual_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- base_price
        latest_base_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.base_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.base_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- estimate_price
        latest_estimate_price AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.estimate_price
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.estimate_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- net_weight
        latest_net_weight AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.net_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.net_weight IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- gross_weight
        latest_gross_weight AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.gross_weight
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.gross_weight IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
         -- FK_tblMaterials_tblProducts
        latest_FK_tblMaterials_tblProducts AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.FK_tblMaterials_tblProducts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblProducts IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- FK_tblMaterials_tblTransportCosts
        latest_FK_tblMaterials_tblTransportCosts AS (
            SELECT hm._rowid, MAX(p.index_num) AS latest_period_index, hm.FK_tblMaterials_tblTransportCosts
            FROM _tblHistoryMaterials hm
            JOIN tblPeriods p ON p.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
            WHERE hm.FK_tblMaterials_tblTransportCosts IS NOT NULL and p.index_num <= :index_number
            GROUP BY hm._rowid
        ),
        -- period
        target_periods AS (
                SELECT p.ID_tblPeriod, p.index_num, p.supplement_num, p.title
                FROM tblPeriods p
                JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                WHERE
                    p.index_num = :index_number
                    AND o.name  = 'ТСН'
                    AND i.team = 'periods_category'
                    AND i.name = 'index'
        )
    SELECT
        hm._rowid AS properties_rowid,
        lidm.ID_tblMaterial AS properties_id,
        tp.index_num AS properties_index_number,
        tp.supplement_num AS properties_supplement_number,
        lbp.base_price AS properties_base_price,
        lep.estimate_price AS properties_estimate_price,
        lap.actual_price AS properties_actual_price,
        lnw.net_weight AS properties_net_weight,
        lgw.gross_weight AS properties_gross_weight,
        lmp.FK_tblMaterials_tblProducts AS properties_product_id,
        lmtc.FK_tblMaterials_tblTransportCosts AS properties_transport_cost_id,
        hm._mask AS properties_mask
    FROM _tblHistoryMaterials hm
    --
    JOIN target_periods tp ON tp.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
    JOIN latest_ID_tblMaterial lidm ON lidm._rowid = hm._rowid
    JOIN latest_actual_price lap ON lap._rowid = hm._rowid
    JOIN latest_estimate_price lep ON lep._rowid = hm._rowid
    JOIN latest_base_price lbp ON lbp._rowid = hm._rowid
    JOIN latest_net_weight lnw ON lnw._rowid = hm._rowid
    JOIN latest_gross_weight lgw ON lgw._rowid = hm._rowid
    JOIN latest_FK_tblMaterials_tblProducts lmp ON lmp._rowid = hm._rowid
    JOIN latest_FK_tblMaterials_tblTransportCosts lmtc ON lmtc._rowid = hm._rowid
    WHERE
        hm._mask != -1
        AND
        lmp.FK_tblMaterials_tblProducts = :product_id
    ;
    """,
    # --- 4 ---
    "select_history_transport_cost_for_period": """--sql
    /*
        Для transport_cost_rowid транспортной затраты и номеру индекса периода выбирает из таблицы истории
        последние изменения этой транспортной затраты. Код транспортной затраты получает из таблицы _tblHistoryProducts
        периода дополнения supplement_number.
        {'transport_cost_rowid': 13, 'index_number': 210, :'supplement_number': 71}
    */
    WITH
        -- ID_TransportCost
        latest_ID_tblTransportCost AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.ID_tblTransportCost
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.ID_tblTransportCost IS NOT NULL and p.index_num <= :index_number
            GROUP BY htc._rowid
        ),
        -- base_price
        latest_base_price AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.base_price
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.base_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY htc._rowid
        ),
        -- actual_price
        latest_actual_price AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.actual_price
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.actual_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY htc._rowid
        ),

        -- FK_tblTransportCosts_tblProducts
        latest_FK_tblTransportCosts_tblProducts AS (
            SELECT htc._rowid, MAX(p.index_num) AS latest_period_index, htc.FK_tblTransportCosts_tblProducts
            FROM _tblHistoryTransportCosts htc
            JOIN tblPeriods p ON p.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
            WHERE htc.FK_tblTransportCosts_tblProducts IS NOT NULL and p.index_num <= :index_number
            GROUP BY htc._rowid
        ),
        -- product_code
            latest_product_code AS (
                SELECT hp._rowid, MAX(p.supplement_num) AS latest_period_supplement, hp.code
                FROM _tblHistoryProducts hp
                JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
                WHERE hp.code IS NOT NULL AND p.supplement_num <= :supplement_number
                GROUP BY hp._rowid
            ),
        -- period
        target_periods AS (
                SELECT p.ID_tblPeriod, p.index_num, p.supplement_num, p.title
                FROM tblPeriods p
                JOIN tblOrigins o ON o.ID_tblOrigin = p.FK_Origin_tblOrigins_tblPeriods
                JOIN tblItems i ON i.ID_tblItem = p.FK_Category_tblItems_tblPeriods
                WHERE
                    p.index_num = :index_number
                    AND o.name  = 'ТСН'
                    AND i.team = 'periods_category'
                    AND i.name = 'index'
        )
    SELECT
        htc._rowid AS transport_cost_rowid,
        tp.index_num AS transport_cost_index_number,
        lidtc.ID_tblTransportCost AS transport_cost_id,
        lbp.base_price AS transport_cost_base_price,
        lap.actual_price AS transport_cost_actual_price,
        ltcp.FK_tblTransportCosts_tblProducts AS transport_cost_product_id,
        lpc.code AS transport_cost_product_code,
        htc._mask AS transport_cost_mask
    FROM _tblHistoryTransportCosts htc
    JOIN target_periods tp ON tp.ID_tblPeriod = htc.FK_tblTransportCosts_tblPeriods
    JOIN latest_ID_tblTransportCost lidtc ON lidtc._rowid = htc._rowid
    JOIN latest_base_price lbp ON lbp._rowid = htc._rowid
    JOIN latest_actual_price lap ON lap._rowid = htc._rowid
    JOIN latest_FK_tblTransportCosts_tblProducts ltcp ON ltcp._rowid = htc._rowid
    JOIN latest_product_code lpc ON lpc._rowid = ltcp.FK_tblTransportCosts_tblProducts
    WHERE
        htc._mask != -1
        AND
        htc._rowid = :transport_cost_rowid
;
    """,
    # ----------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------
    "select_period_id_for_max_index": """--sql
        -- получить id периода Материалов у которого индекс периода максимальный
        SELECT period.ID_tblPeriod AS period_id, MAX(period.index_num) AS max_index
        FROM tblMaterials m
        JOIN tblPeriods AS period ON period.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE m.base_price > 0
        GROUP BY period.ID_tblPeriod;
    """,
    "select_id_records_for_period_id": """--sql
        -- получить id записей Материалов для периода у которых базовая цена != 0
        -- ? id периода
        SELECT m.FK_tblMaterials_tblProducts AS materials_id
        FROM tblMaterials m
        JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        WHERE p.ID_tblPeriod = ? AND m.base_price > 0;
    """,
    #
    "select_historical_prices_for_materials_id_not_empty_actual_price": """--sql
        /*
        получить историю изменения цен Материалов по id.
        только тогда когда меняется период. Если нет актуальной цены то берем ближайшую цену из истории
        ? id записи для которой строится история
        ? кол-во истории
        сортировка от большего к меньшему иначе получим раннюю историю а не последнюю в глубину
        */
        SELECT
            hm._version AS "version",
            period.index_num,
            hm._rowid AS id,
            hm.FK_tblMaterials_tblPeriods AS "period_id",
            hm.base_price,
            COALESCE(hm.actual_price, (
                SELECT actual_price
                FROM _tblHistoryMaterials
                WHERE
                    _rowid = hm._rowid
                    AND _version < hm._version
                    AND actual_price IS NOT NULL
                ORDER BY _version DESC
                LIMIT 1
            )) AS actual_price
        FROM _tblHistoryMaterials hm
        JOIN tblPeriods AS period ON period.ID_tblPeriod = hm.FK_tblMaterials_tblPeriods
        WHERE
            hm._rowid = ?
            AND hm.FK_tblMaterials_tblPeriods IS NOT NULL
        ORDER BY hm._version DESC
        LIMIT ?;
    """,
    #
    "select_records_for_max_index_with_monitoring": """--sql
        -- получить Материалы у которых базовая цена != 0 и индекс периода максимальный
        WITH vars(period_id, max_period_index) AS (
            SELECT p.ID_tblPeriod, MAX(p.index_num)
            FROM tblMaterials m
            JOIN tblPeriods AS p ON p.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
            WHERE m.base_price > 0
            GROUP BY p.ID_tblPeriod
        )
        SELECT
            materials.ID_tblMaterial,
            periods.index_num,
            products.code,
            products.description AS title,
            --
            materials.net_weight,
            materials.gross_weight,
            materials.base_price,
            materials.actual_price,
            monitoring.supplier_price AS monitoring_price,
            monitoring.FK_tblMonitoringMaterial_tblPeriods AS monitoring_period_id,
            (SELECT index_num FROM tblPeriods WHERE ID_tblPeriod = monitoring.FK_tblMonitoringMaterial_tblPeriods) AS monitoring_index_num,
            monitoring.delivery AS transport_flag,
            --
            (
               SELECT ptr.code
               FROM tblProducts AS ptr
               WHERE ptr.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
            ) AS transport_code,
            COALESCE(tc.base_price, 0) AS transport_base_price,
            COALESCE(tc.inflation_ratio, 0) AS transport_factor

        FROM tblMaterials materials
        JOIN tblPeriods AS periods ON periods.ID_tblPeriod = materials.FK_tblMaterials_tblPeriods
        JOIN tblProducts AS products ON products.ID_tblProduct = materials.FK_tblMaterials_tblProducts
        LEFT JOIN tblMonitoringMaterials AS monitoring ON monitoring.FK_tblMonitoringMaterial_tblProducts = products.ID_tblProduct
        LEFT JOIN tblTransportCosts AS tc ON tc.ID_tblTransportCost = materials.FK_tblMaterials_tblTransportCosts
        JOIN vars ON vars.period_id = periods.ID_tblPeriod
        WHERE periods.index_num = vars.max_period_index AND materials.base_price > 0
        ORDER BY products.digit_code ASC
        --LIMIT 10
        ;
    """,
    #  Products history get
    "select_history_product_for_period": """--sql
    /*
        для id продукта и номеру дополнения периода выбирает из таблицы истории
        последние изменения в данных этого продукта для периода
        {'rowid': 5917, 'supplement_number': 70}
    */
    WITH
        -- ID_tblProduct
        latest_id AS (
            SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_id, ID_tblProduct
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.ID_tblProduct IS NOT NULL and p.supplement_num <= :supplement_number
            GROUP BY _rowid
        ),
        -- code
        latest_code AS (
            SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_code, code
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.code IS NOT NULL and p.supplement_num <= :supplement_number
            GROUP BY _rowid
        ),
        -- description
        latest_description AS (
            SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_description, description
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.description IS NOT NULL and p.supplement_num <= :supplement_number
            GROUP BY _rowid
        ),
        -- measurer
        latest_measurer AS (
            SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_measurer, measurer
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.measurer IS NOT NULL and p.supplement_num <= :supplement_number
            GROUP BY _rowid
        ),
        -- digit_code
        latest_digit_code AS (
            SELECT _rowid, p.ID_tblPeriod, MAX(p.supplement_num) AS latest_period_digit_code, digit_code
            FROM _tblHistoryProducts hp
            JOIN tblPeriods p ON p.ID_tblPeriod = hp.FK_tblProducts_tblPeriods
            WHERE hp.digit_code IS NOT NULL and p.supplement_num <= :supplement_number
            GROUP BY _rowid
        ),
        -- period
        target_periods AS (
            SELECT p.ID_tblPeriod, supplement_num
            FROM tblPeriods p
            WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'ТСН')
                AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'supplement')
                AND p.supplement_num = :supplement_number
        )

    SELECT
        hp._rowid AS product_rowid, tp.supplement_num, lid.ID_tblProduct,
        lc.code, ld.description, lm.measurer
        -- , ldc.digit_code
    FROM _tblHistoryProducts hp
    --
    JOIN target_periods tp ON tp.ID_tblPeriod = hp.FK_tblProducts_tblPeriods

    JOIN latest_id lid ON lid._rowid = hp._rowid
    JOIN latest_code lc ON lc._rowid = hp._rowid
    JOIN latest_description ld ON ld._rowid = hp._rowid
    JOIN latest_measurer lm ON lm._rowid = hp._rowid
    JOIN latest_digit_code ldc ON ldc._rowid = hp._rowid
    WHERE hp._rowid = :rowid
    --ORDER BY ldc.digit_code
    ;
    """,
    #
    #
    "select_history_monitoring_materials_for_period": """--sql
    /*
        для id продукта и номеру дополнения периода выбирает из таблицы истории
        последние изменения в данных этого продукта для периода
        {'rowid': 5917, 'index_number': 208}
    */
    WITH
        -- FK_tblMonitoringMaterial_tblProducts
        latest_FK_tblMonitoringMaterial_tblProducts AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_product_id, FK_tblMonitoringMaterial_tblProducts
            FROM  _tblHistoryMonitoringMaterials
            JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
            WHERE FK_tblMonitoringMaterial_tblProducts IS NOT NULL and p.index_num <= 211
            GROUP BY _rowid
        ),
        -- supplier_price
        latest_supplier_price AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_supplier_price, supplier_price
            FROM  _tblHistoryMonitoringMaterials
            JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
            WHERE supplier_price IS NOT NULL and p.index_num <= :index_number
            GROUP BY _rowid
        ),
        -- delivery
        latest_delivery AS (
            SELECT _rowid, MAX(p.index_num) AS latest_period_delivery, delivery
            FROM  _tblHistoryMonitoringMaterials
            JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
            WHERE delivery IS NOT NULL and p.index_num <= :index_number
            GROUP BY _rowid
        ),
        /*
        -- title
        -- если везде null, то выбираем весь запрос работать не будет
        latest_title AS (
            SELECT mm1._rowid, MAX(p.index_num) AS latest_period_title, mm1.title
            FROM  _tblHistoryMonitoringMaterials mm1
            JOIN tblPeriods p ON p.ID_tblPeriod = FK_tblMonitoringMaterial_tblPeriods
            WHERE mm1.title IS NOT NULL AND p.index_num <= :index_number
            GROUP BY mm1._rowid
        ),*/
        -- period
        target_periods AS (
            SELECT p.ID_tblPeriod, index_num
            FROM tblPeriods p
            WHERE FK_Origin_tblOrigins_tblPeriods = (SELECT ID_tblOrigin FROM tblOrigins WHERE name = 'мониторинг')
                AND FK_Category_tblItems_tblPeriods = (SELECT ID_tblItem FROM tblItems WHERE name = 'index')
                AND p.index_num = :index_number
        )
    SELECT
        hmm._rowid AS monitoring_material_rowid,
        tp.index_num AS monitoring_materials_index_number,
        hmm.ID_tblMonitoringMaterial,
        lsp.supplier_price AS monitoring_supplier_price,
        ld.delivery AS monitoring_delivery,
        --lt.title AS monitoring_title,
        lmmp.FK_tblMonitoringMaterial_tblProducts AS monitoring_product_id
    FROM _tblHistoryMonitoringMaterials hmm
    JOIN target_periods tp ON tp.ID_tblPeriod = hmm.FK_tblMonitoringMaterial_tblPeriods
    JOIN latest_FK_tblMonitoringMaterial_tblProducts lmmp ON lmmp._rowid = hmm._rowid
    JOIN latest_supplier_price lsp ON lsp._rowid = hmm._rowid
    JOIN latest_delivery ld ON ld._rowid = hmm._rowid
    --JOIN latest_title lt ON lt._rowid = hmm._rowid
    WHERE lmmp.FK_tblMonitoringMaterial_tblProducts = :rowid
    ;
    """,
}