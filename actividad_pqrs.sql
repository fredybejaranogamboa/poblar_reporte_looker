CREATE TABLE pqrsf_actividad (
	id INT AUTO_INCREMENT NOT NULL,
	nomact VARCHAR(255) NULL,
	total_dias INT NULL,
	numpro_cnt INT NULL,
	avg_dias DOUBLE NULL,
	max_dias INT NULL,
	min_dias INT NULL,
	rango_dias INT NULL,
	varianza DOUBLE NULL,
	desv_std DOUBLE NULL,
	moda INT NULL,
	numpro_max_dias INT NULL,
	PRIMARY KEY (id)
);
