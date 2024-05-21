
sql_slice_materials = {
    "delete_table_slice_materials": """--sql
        DROP TABLE IF EXISTS tblSliceMaterials;
    """,
    "create_table_slice_materials": """--sql
        CREATE TABLE tblSliceMaterials (
            product_rowid INTEGER PRIMARY KEY,
            product_id INTEGER,
            product_period_id INTEGER,
            product_code TEXT,
            product_description TEXT,
            product_measurer TEXT,
            product_digit_code INTEGER,
            catalog_id INTEGER,
            catalog_code TEXT,
            UNIQUE (product_period_id, product_id)
        );
    """,
    "create_index_slice_materials": """--sql
        CREATE INDEX idxSliceMaterials ON tblSliceMaterials (product_id, product_period_id);
    """,
    "insert_slice_materials": """--sql
        INSERT INTO tblSliceMaterials (
                product_rowid, product_id, product_period_id,
                product_code, product_description, product_measurer,
                product_digit_code, catalog_id, catalog_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    # ----------------------------------------------
    # Properties of materials
    # ----------------------------------------------
    "delete_table_properties_materials": """--sql
        DROP TABLE IF EXISTS tblPropertiesMaterials;
    """,
    "create_table_properties_materials": """--sql
        CREATE TABLE tblPropertiesMaterials (
            properties_rowid INTEGER PRIMARY KEY,
            properties_product_id INTEGER,
            properties_period_id INTEGER,
            transport_cost_id INTEGER,
            actual_price REAL,
            estimate_price REAL,
            base_price REAL,
            net_weight REAL,
            gross_weight REAL,
            UNIQUE (properties_period_id, properties_product_id)
        );
    """,
    "create_index_properties_materials": """--sql
        CREATE INDEX idxPropertiesMaterials ON tblPropertiesMaterials (properties_period_id, properties_product_id);
    """,
    "insert_properties_materials": """--sql
        INSERT INTO tblPropertiesMaterials (
            properties_rowid,
            properties_product_id,
            properties_period_id,
            transport_cost_id,
            actual_price,
            estimate_price,
            base_price,
            net_weight,
            gross_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    # ----------------------------------------------
    # Transportation costs
    # ----------------------------------------------
    "delete_table_slice_transport_costs": """--sql
        DROP TABLE IF EXISTS tblSliceTransportCosts;
    """,
    "create_table_slice_transport_costs": """--sql
        CREATE TABLE tblSliceTransportCosts (
            transport_cost_rowid INTEGER PRIMARY KEY,
            transport_cost_id INTEGER,
            transport_cost_period_id INTEGER,
            transport_cost_base_price REAL,
            transport_cost_actual_price REAL,
            transport_cost_product_id INTEGER,
            transport_cost_code TEXT,
            transport_cost_description TEXT,
            UNIQUE (transport_cost_period_id, transport_cost_id)
        );
    """,
    "create_index_slice_transportation_costs": """--sql
        CREATE INDEX idxSliceTransportCosts ON tblSliceTransportCosts (transport_cost_period_id, transport_cost_id);
    """,
    "insert_slice_transportation_costs": """--sql
        INSERT INTO tblSliceTransportCosts (
            transport_cost_rowid,
            transport_cost_id,
            transport_cost_period_id,
            transport_cost_base_price,
            transport_cost_actual_price,
            transport_cost_product_id,
            transport_cost_code,
            transport_cost_description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """,
    # ----------------------------------------------
    # Monitoring materials
    # ----------------------------------------------
    "delete_table_slice_monitoring_materials": """--sql
        DROP TABLE IF EXISTS tblSliceMonitoringMaterials;
    """,
    "create_table_slice_monitoring_materials": """--sql
        CREATE TABLE tblSliceMonitoringMaterials (
            monitoring_material_rowid INTEGER PRIMARY KEY,
            monitoring_material_id INTEGER,
            monitoring_materials_period_id INTEGER,
            monitoring_supplier_price REAL,
            monitoring_delivery INTEGER,
            monitoring_materials_title TEXT,
            monitoring_product_id INTEGER,
            monitoring_product_code TEXT,
            monitoring_digit_code INTEGER,
            UNIQUE (monitoring_materials_period_id, monitoring_material_id)
        );
    """,
    "create_index_slice_monitoring_materials": """--sql
        CREATE INDEX idxSliceMonitoringMaterials ON tblSliceMonitoringMaterials (monitoring_materials_period_id, monitoring_material_id);
    """,
    "insert_slice_monitoring_materials": """--sql
        INSERT INTO tblSliceMonitoringMaterials (
            monitoring_material_rowid,
            monitoring_material_id,
            monitoring_materials_period_id,
            monitoring_supplier_price,
            monitoring_delivery,
            monitoring_materials_title,
            monitoring_product_id,
            monitoring_product_code,
            monitoring_digit_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    # ----------------------------------------------
    # ----------------------------------------------
    "select_result": """--sql
        SELECT
            ps.supplement_num,
            pi.index_num,
            sm.catalog_code,
            sm.product_code,
            sm.product_description,
            sm.product_measurer,
            actual_price,
            estimate_price,
            base_price,
            net_weight,
            gross_weight,
            transport_cost_base_price,
            transport_cost_actual_price,
            transport_cost_code,
            transport_cost_description
        FROM tblSliceMaterials sm
        JOIN tblPropertiesMaterials prop ON prop.properties_product_id = sm.product_id
        JOIN tblSliceTransportCosts tc ON tc.transport_cost_id = prop.transport_cost_id
        JOIN tblPeriods pi ON pi.ID_tblPeriod = prop.properties_period_id
        JOIN tblPeriods ps ON ps.ID_tblPeriod = sm.product_period_id
        ORDER BY sm.product_digit_code
        ;
    """,
}
