def transfer_raw_data_to_quotes(db_filename: str):
    """ Записывает расценки из сырой таблицы tblRawData в рабочую таблицу tblProducts.
        В рабочей таблице tblProducts ищется расценка с таким же шифром, если такая есть то она обновляется,
        если не найдена то вставляется новая.
     """
    with dbTolls(db_filename) as db:
        raw_cursor = db.go_execute(sql_raw_queries["select_rwd_all"])
        raw_data = raw_cursor.fetchall() if raw_cursor else None
        if not raw_data:
            output_message_exit(f"в RAW таблице с данными для Расценок не найдено ни одной записи:",
                                f"tblRawData пустая")
            return None

        type = db.get_row_id(sql_items_queries["select_items_dual_teams"], ("units", "quote"))

        inserted_success, updated_success = [], []
        for row_count, row in enumerate(raw_data):
            raw_code = clear_code(row["PRESSMARK"])
            raw_period = get_integer_value(row["PERIOD"])
            # Найти запись с шифром raw_cod в таблице расценок tblProducts
            work_cursor = db.go_execute(sql_products_queries["select_products_item_code"], (raw_code,))
            work_row = work_cursor.fetchone() if work_cursor else None
            if work_row:
                work_period = work_row['period']
                work_id = work_row['ID_tblQuote']
                if raw_period >= work_period:
                    work_id = _update_quote(db, work_id, row)
                    if work_id:
                        updated_success.append((id, raw_code))
                else:
                    output_message_exit(
                        f"Ошибка загрузки Расценки с шифром: {raw_code!r}",
                        f"текущий период Расценки {work_period} старше загружаемого {raw_period}")
            else:
                work_id = _insert_raw_quote(db, row)
                if work_id:
                    inserted_success.append((id, raw_code))
        row_count += 1
        alog = f"Всего записей в raw таблице: {raw_count}."
        ilog = f"Добавлено {len(inserted_success)} расценок."
        ulog = f"Обновлено {len(updated_success)} расценок."
        none_log = f"Непонятных записей: {raw_count - (len(updated_success) + len(inserted_success))}."
        ic(alog, ilog, ulog, none_log)

    # удалить из Расценок записи период которых меньше чем максимальный период
    # _delete_last_period_quotes_row(db_filename)
