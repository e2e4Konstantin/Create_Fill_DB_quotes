SELECT per.title AS период,
           pr.code AS шифр,
           --m.RPC AS ОКП,
           --m.RPCA2 AS ОКПД2,
           pr.description AS название,
           pr.measurer AS [ед.измер],-- 
           m.net_weight AS нетто,
           m.gross_weight AS брутто,
           m.base_price AS [баз.цена],
           m.actual_price AS [отп.тек.цена],-- ОТЦ: Отпускная Текущая Цена (от мониторинга)
           m.estimate_price AS [смет.тек.цена],-- СТЦ: Сметная Текущая Цена (расчетная)
           m.inflation_ratio AS [коэф.инфл],-- инфляция estimate_price/base_price
           m.calc_estimate_price AS [смет.обрат.цена],-- обратный пересчет СТЦ (estimate_price/base_price)*base_price
           /*  */COALESCE(tc.base_price, 0) AS [Транс.Баз.цена],
           COALESCE(tc.actual_price, 0) AS [Транс.Тек.цена],
           COALESCE(tc.inflation_ratio, 0) AS [Транс.Коэф.Инф],
           (select code from tblProducts where ID_tblProduct = tc.FK_tblTransportCosts_tblProducts) as trans_code
      FROM tblMaterials m
           LEFT JOIN
           tblProducts AS pr ON pr.ID_tblProduct = m.FK_tblMaterials_tblProducts
           LEFT JOIN
           tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
           LEFT JOIN
           tblTransportCosts AS tc ON tc.ID_tblTransportCost = m.FK_tblMaterials_tblTransportCosts
     ORDER BY m.FK_tblMaterials_tblPeriods,
              pr.digit_code;
              



        SELECT
            per.title AS 'период',
            pr.code AS 'шифр',
            m.RPC AS 'ОКП',
            m.RPCA2  AS 'ОКПД2',
            pr.description AS 'название',
            pr.measurer AS 'ед.измер',
            --
            m.net_weight AS 'нетто',
            m.gross_weight AS 'брутто',
            m.base_price AS 'баз.цена',
            m.actual_price AS 'отп.тек.цена',                        -- ОТЦ: Отпускная Текущая Цена (от мониторинга)
            m.estimate_price AS 'смет.тек.цена',                     -- СТЦ: Сметная Текущая Цена (расчетная)
            m.inflation_ratio AS 'коэф.инфл',                        -- инфляция estimate_price/base_price
            m.calc_estimate_price AS 'смет.обрат.цена',              -- обратный пересчет СТЦ (estimate_price/base_price)*base_price
            --
            (SELECT ptr.code FROM tblProducts AS ptr WHERE ptr.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts) AS 'код транспортировки',
            COALESCE(tc.base_price, 0) AS "Транс.Баз.цена",
            COALESCE(tc.actual_price, 0) AS "Транс.Тек.цена",
            COALESCE(tc.inflation_ratio, 0) AS "Транс.Коэф.Инф"
        FROM tblMaterials m
        LEFT JOIN tblProducts AS pr ON pr.ID_tblProduct = m.FK_tblMaterials_tblProducts
        LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = m.FK_tblMaterials_tblPeriods
        LEFT JOIN tblTransportCosts AS tc ON  tc.ID_tblTransportCost = m.FK_tblMaterials_tblTransportCosts
        ORDER BY m.FK_tblMaterials_tblPeriods, pr.digit_code;
        


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
        LIMIT 10;