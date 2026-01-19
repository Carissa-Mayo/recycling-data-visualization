create schema if not exists staging;
create schema if not exists mart;

drop table if exists staging.rdrs_long;
create table staging.rdrs_long (
	year	int not null,
	quarter	int not null check (quarter between 1 and 4),
	yearquarter text not null,
	source text not null,
	entity text not null,
	stream text not null,
	ton		double precision not null
);
