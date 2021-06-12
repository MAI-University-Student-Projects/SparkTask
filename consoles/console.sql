create table dim_date
(
	dim_date_id integer not null
		constraint DIM_DATE_PK
			primary key,
	date_val date,
	date_lvl integer
);

create table dim_tags
(
	dim_tag_id integer not null
		constraint DIM_TAGS_PK
			primary key,
	tag_name varchar2(100) not null
);

create table fact_questioned
(
    d_date_id integer
        constraint DIM_DATE_FK
            references DIM_DATE,
    d_tag_id integer
        constraint DIM_TAG_FK
            references DIM_TAGS,
    fact_id integer,
    views_amount integer,
    answers_amount integer,
    total_amount integer,
    score integer
);

