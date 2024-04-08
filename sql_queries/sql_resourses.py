

CREATE TABLE larix.resources (
	"period" int8 NOT NULL,
	id int8 NOT NULL,
	type_resource int8 NOT NULL,
	title varchar(700) NOT NULL,
	pressmark varchar(256) NOT NULL,
	unit_of_measure int8 NOT NULL,
	okp varchar(200) NULL,
	okpsn varchar(200) NULL,
	okpd2 varchar(200) NULL,
	netto numeric(19, 8) DEFAULT 0 NOT NULL,
	brutto numeric(19, 8) DEFAULT 0 NOT NULL,
	power_expenditure numeric(19, 8) DEFAULT 0 NULL,
	group_resource int8 NOT NULL,
	cmt varchar(200) NULL,
	created_on timestamp DEFAULT LOCALTIMESTAMP NOT NULL,
	created_by varchar(50) NOT NULL,
	modified_on timestamp DEFAULT LOCALTIMESTAMP NULL,
	modified_by varchar(50) NULL,
	version_number int8 DEFAULT 1 NOT NULL,
	deleted int2 DEFAULT 0 NOT NULL,
	resource_calc_method int8 DEFAULT 0 NOT NULL,
	transport_cost int8 DEFAULT 0 NOT NULL,
	price numeric(19, 8) DEFAULT 0 NOT NULL,
	cur_price numeric(19, 8) DEFAULT 0 NOT NULL,
	long_title varchar(1000) NULL,
	status int8 DEFAULT 1 NOT NULL,
	storage_cost int8 DEFAULT 0 NOT NULL,
	tech_descr varchar(4000) NULL,
	eq_transport_pc numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_salary numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_salary numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_depr_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_depr_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_service_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_service_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_replace_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_replace_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_energy_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_energy_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_rebase_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	ma_cur_rebase_cost numeric(19, 8) DEFAULT 0 NOT NULL,
	calculated int2 DEFAULT 0 NOT NULL,
	cur_sale_price numeric(19, 8) DEFAULT 0 NOT NULL,
	prev_sale_price numeric(19, 8) DEFAULT 0 NOT NULL,
	resource_price_list int8 NULL,
	use_to_calc_avg_rate int2 DEFAULT 1 NULL,
	worker_grade int8 DEFAULT 0 NULL,
	sale_price numeric(19, 8) DEFAULT 0 NOT NULL,
	drivers_count int8 DEFAULT 0 NOT NULL,
	deleted_on timestamp NULL,
	pressmark2022 varchar(256) NULL,
	subtype_resource int8 NOT NULL,
	pressmark_sort numeric(22) GENERATED ALWAYS AS (larix.get_pressmark_sort4(pressmark)) STORED NULL,
	infl_rate numeric(19, 8) GENERATED ALWAYS AS (
CASE
    WHEN price = 0::numeric THEN 0::numeric
    ELSE round(cur_price / price, 2)
END) STORED NULL,
	cur_price_calc numeric(19, 8) GENERATED ALWAYS AS (round(
CASE
    WHEN price = 0::numeric THEN 0::numeric
    ELSE round(cur_price / price, 2)
END * price, 2)) STORED NULL,
	ma_salary_infl_rate numeric(19, 8) GENERATED ALWAYS AS (
CASE
    WHEN ma_salary = 0::numeric THEN 0::numeric
    ELSE round(ma_cur_salary / ma_salary, 2)
END) STORED NULL,
	ma_cur_salary_calc numeric(19, 8) GENERATED ALWAYS AS (round(
CASE
    WHEN ma_salary = 0::numeric THEN 0::numeric
    ELSE round(ma_cur_salary / ma_salary, 2)
END * ma_cur_salary, 2)) STORED NULL,
	CONSTRAINT resources_period_pressmark_key UNIQUE (period, pressmark),
	CONSTRAINT resources_pkey PRIMARY KEY (period, id),
	CONSTRAINT fk_fk_period_resources FOREIGN KEY ("period") REFERENCES larix."period"(id),
	CONSTRAINT fk_group_resource_resources FOREIGN KEY (group_resource,"period") REFERENCES larix.group_resource(id,"period"),
	CONSTRAINT fk_res_calc_method_resource FOREIGN KEY (resource_calc_method) REFERENCES larix.resource_calc_method(id),
	CONSTRAINT fk_resource_price_list FOREIGN KEY ("period",resource_price_list) REFERENCES larix.resource_price_list("period",id),
	CONSTRAINT fk_resources__type_resource_sub FOREIGN KEY (subtype_resource) REFERENCES larix.type_resource(id),
	CONSTRAINT fk_status_resource FOREIGN KEY (status) REFERENCES larix.status(id),
	CONSTRAINT fk_storage_cost_resource FOREIGN KEY ("period",storage_cost) REFERENCES larix.storage_cost("period",id),
	CONSTRAINT fk_transport_cost_resources FOREIGN KEY (transport_cost,"period") REFERENCES larix.transport_cost(id,"period"),
	CONSTRAINT fk_type_resource_resources FOREIGN KEY (type_resource) REFERENCES larix.type_resource(id),
	CONSTRAINT fk_unit_of_measure_resources FOREIGN KEY (unit_of_measure) REFERENCES larix.unit_of_measure(id)
)