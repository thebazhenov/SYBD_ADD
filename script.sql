DO $$
DECLARE
  kbks VARCHAR[];
  start_date DATE[];
  end_date DATE[];
  kbk_id UUID;
  kbk varchar;
  deb_id uuid;
  cred_id uuid;

BEGIN
  kbks := ARRAY['90111105034040007120',
'90111402043040000440',
'90111402042040000410',
'90120225576040000150',
'90120229999040000150',
'90111402043040001410',
'90111607010040000140',
'90111301994040004130']; -- Список КБК из файлов
  start_date := ARRAY['2022-01-01','2023-01-01','2024-01-01']; -- Дата начала распределения. ВАЖНО! Массив итерируется вместе с массивом end_date не проебитесь с индексами
  end_date := ARRAY['2022-12-31','2023-12-31','2024-12-31']; -- Дата окончания распределения. Вот этот массив с которым нужно не проебаться в сопоставление индексов
  deb_id = 'e4043d41-45b6-4af0-b88e-1d3a94a51922'; -- Указывается нужный нам деб. uuid из sbudget_operation_type
  cred_id = 'abdbc0f9-0e18-4b95-bd3a-eecba3aa1400'; -- Указывается нужный нам кред. uuid из sbudget_operation_type

  FOREACH kbk IN ARRAY kbks
  LOOP
    WITH insert_return AS (
      INSERT INTO public.skbk
      (descr, value, kbk_id, update_time, is_sync, sync_time, sync_pays_to_mo, oktmo_to_sync, date_budget_close)
    VALUES('CУФД.Невыясненные', kbk, gen_random_uuid(), current_timestamp, false, NULL, false, '', NULL)
    RETURNING skbk.kbk_id
    )

    SELECT ir.kbk_id INTO kbk_id FROM insert_return ir;

    FOR i IN 1..array_length(start_date, 1)
    LOOP
      -- Не проебись здесь!!!!"!!!!" Нужно выбрать распределение куда ебашишь
	  INSERT INTO public.skbk_raspr
      (kbk_raspr_id, date_start, date_end, percent_1, percent_2, percent_3, percent_4, percent_5, percent_6, percent_7, percent_8, percent_9, update_time, sync_time, is_sync, kbk_id)
      VALUES(gen_random_uuid(), start_date[i], end_date[i], 0.00, 0.00, 0.00, 100, 0.00, 0.00, 100, 0.00, 0.00, current_timestamp, NULL, false, kbk_id);
    END LOOP;

    INSERT INTO public.sbudget_operation_type
	(budget_operation_type_id, kbk, update_time, is_sync, sync_time, debet_account_id, credit_account_id, journal_number, descr, "type")
	VALUES(gen_random_uuid(), kbk, current_timestamp, false, NULL, deb_id, cred_id, '0', 'СУФД.Невыясненные', 0);

  END LOOP;
END $$;