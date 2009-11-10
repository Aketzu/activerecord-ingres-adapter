\nocontinue
create sequence accounts_id;\p\g
create table accounts(
	id integer not null default accounts_id.nextval primary key,
	firm_id integer,
	credit_limit integer
)
\p\g
create sequence audit_logs_id\p\g
create table audit_logs(
	id integer not null default audit_logs_id.nextval primary key,
	"message" varchar(255) not null,
	developer_id integer not null
)
\p\g
create sequence author_addresses_id\p\g
create table author_addresses(
	id integer not null default author_addresses_id.nextval primary key
)
\p\g
create sequence author_favorites_id\p\g
create table author_favorites(
	id integer not null default author_favorites_id.nextval primary key,
	author_id integer,
	favorite_author_id integer
)
\p\g
create sequence authors_id;\p\g
create table authors(
	id integer not null default authors_id.nextval primary key,
	name varchar(255) not null,
	author_address_id integer,
	author_address_extra_id integer
)
\p\g
create sequence auto_id_tests_id;\p\g
create table auto_id_tests(
	auto_id integer not null default auto_id_tests_id.nextval primary key,
	value integer
)
\p\g
create sequence binaries_id;\p\g
create table binaries(
	id integer not null default binaries_id.nextval primary key,
	data long byte
)
\p\g
create sequence birds_id;\p\g
create table birds(
	id integer not null default birds_id.nextval primary key,
	name varchar(255),
	pirate_id integer
)
\p\g
create sequence books_id;\p\g
create table books(
	id integer not null default books_id.nextval primary key,
	name varchar(255)
)
\p\g
create sequence booleantests_id;\p\g
create table booleantests(
	id integer not null default booleantests_id.nextval primary key,
	value i1
)
\p\g
create sequence camelcase_id;\p\g
create table camelcase(
	id integer not null default camelcase_id.nextval primary key,
	name varchar(255)
)
\p\g
create sequence categories_id;\p\g
create table categories(
	id integer not null default categories_id.nextval primary key,
	name varchar(255) not null,
	type varchar(255),
	categorizations_count integer
)
\p\g
create table categories_posts(
	category_id integer not null,
	post_id integer not null
)
\p\g
create sequence categorizations_id;\p\g
create table categorizations(
	id integer not null default categorizations_id.nextval primary key,
	category_id integer,
	post_id integer,
	author_id integer
)
\p\g
create sequence circles_id;\p\g
create table circles(
	id integer not null default circles_id.nextval primary key
)
\p\g
create sequence citations_id;\p\g
create table citations(
	id integer not null default citations_id.nextval primary key,
	book1_id integer,
	book2_id integer
)
\p\g
create sequence clubs_id;\p\g
create table clubs(
	id integer not null default clubs_id.nextval primary key,
	name varchar(255)
)
\p\g
create sequence colnametests_id;\p\g
create table colnametests(
	id integer not null default colnametests_id.nextval primary key,
	"references" integer not null
)
\p\g
create sequence comments_id;\p\g
create table comments(
	id integer not null default comments_id.nextval primary key,
	post_id integer not null,
	body varchar(32000) not null,
	type varchar(255)
)
\p\g
create sequence companies_id;\p\g
create table companies(
	id integer not null default companies_id.nextval primary key,
	type varchar(255),
	ruby_type varchar(255),
	firm_id integer,
	firm_name varchar(255),
	name varchar(255),
	client_of integer,
	rating integer default '1'
)
\p\g
create sequence computers_id;\p\g
create table computers(
	id integer not null default computers_id.nextval primary key,
	developer integer not null,
	extendedwarranty integer not null
)
\p\g
create sequence contracts_id;\p\g
create table contracts(
	id integer not null default contracts_id.nextval primary key,
	developer_id integer,
	company_id integer
)
\p\g
create sequence customers_id;\p\g
create table customers(
	id integer not null default customers_id.nextval primary key,
	name varchar(255),
	balance integer default '0',
	address_street varchar(255),
	address_city varchar(255),
	address_country varchar(255),
	gps_location varchar(255)
)
\p\g
create sequence developers_id;\p\g
create table developers(
	id integer not null default developers_id.nextval primary key,
	name varchar(255),
	salary integer default '70000',
	created_at ingresdate,
	updated_at ingresdate
)
\p\g
create table developers_projects(
	developer_id integer not null,
	project_id integer not null,
	joined_on ansidate,
	access_level integer default '1'
)
\p\g
create sequence edges_id;\p\g
create table edges(
	id integer not null default edges_id.nextval primary key,
	source_id integer not null,
	sink_id integer not null
)
\p\g
create sequence entrants_id;\p\g
create table entrants(
	id integer not null default entrants_id.nextval primary key,
	name varchar(255) not null,
	course_id integer not null
)
\p\g
create sequence essays_id;\p\g
create table essays(
	id integer not null default essays_id.nextval primary key,
	name varchar(255),
	writer_id varchar(255),
	writer_type varchar(255)
)
\p\g
create sequence events_id;\p\g
create table events(
	id integer not null default events_id.nextval primary key,
	title varchar(5)
)
\p\g
create sequence fk_test_has_pk_id;\p\g
create table fk_test_has_pk(
	id integer not null default fk_test_has_pk_id.nextval primary key
)
\p\g
create sequence fk_test_has_fk_id;\p\g
create table fk_test_has_fk(
	id integer not null default fk_test_has_fk_id.nextval primary key,
	fk_id integer not null,
    unique (fk_id),
    foreign key (fk_id) references fk_test_has_pk (id)
)
\p\g
create sequence funny_jokes_id;\p\g
create table funny_jokes(
	id integer not null default funny_jokes_id.nextval primary key,
	name varchar(255)
)
\p\g
create table goofy_string_id(
	id varchar(255) not null,
	info varchar(255)
)
\p\g
create sequence guids_id;\p\g
create table guids(
	id integer not null default guids_id.nextval primary key,
	"key" varchar(255)
)
\p\g
create sequence inept_wizards_id;\p\g
create table inept_wizards(
	id integer not null default inept_wizards_id.nextval primary key,
	name varchar(255) not null,
	city varchar(255) not null,
	type varchar(255)
)
\p\g
create sequence integer_limits_id;\p\g
create table integer_limits(
	id integer not null default integer_limits_id.nextval primary key,
	c_int_without_limit integer,
	c_int_1 smallint,
	c_int_2 smallint,
	c_int_3 integer,
	c_int_4 integer,
	c_int_5 i8,
	c_int_6 i8,
	c_int_7 i8,
	c_int_8 i8
)
\p\g
create sequence items_id;\p\g
create table items(
	id integer not null default items_id.nextval primary key,
	name integer
)
\p\g
create sequence jobs_id;\p\g
create table jobs(
	id integer not null default jobs_id.nextval primary key,
	ideal_reference_id integer
)
\p\g
create sequence keyboards_key_number;\p\g
create table keyboards(
	key_number integer not null default keyboards_key_number.nextval primary key,
	name varchar(255)
)
\p\g
create sequence legacy_things_id;\p\g
create table legacy_things(
	id integer not null default legacy_things_id.nextval primary key,
	tps_report_number integer,
	version integer not null default '0'
)
\p\g
create sequence lock_without_defaults_id\p\g
create table lock_without_defaults(
	id integer not null default lock_without_defaults_id.nextval primary key,
	lock_version integer
)
\p\g
create sequence lock_without_defaults_cust_id\p\g
create table lock_without_defaults_cust(
	id integer not null default lock_without_defaults_cust_id.nextval primary key,
	custom_lock_version integer
)
\p\g
create table mateys(
	pirate_id integer,
	target_id integer,
	weight integer
)
\p\g
create sequence member_details_id;\p\g
create table member_details(
	id integer not null default member_details_id.nextval primary key,
	member_id integer,
	organization_id integer,
	extra_data varchar(255)
)
\p\g
create sequence member_types_id;\p\g
create table member_types(
	id integer not null default member_types_id.nextval primary key,
	name varchar(255)
)
\p\g
create sequence members_id;\p\g
create table members(
	id integer not null default members_id.nextval primary key,
	name varchar(255),
	member_type_id integer
)
\p\g
create sequence memberships_id;\p\g
create table memberships(
	id integer not null default memberships_id.nextval primary key,
	joined_on ingresdate,
	club_id integer,
	member_id integer,
	favourite i1 default 0,
	type varchar(255)
)
\p\g
create sequence minimalistics_id;\p\g
create table minimalistics(
	id integer not null default minimalistics_id.nextval primary key
)
\p\g
create sequence mixed_case_monkeys_id;\p\g
create table mixed_case_monkeys(
	"monkeyID" integer not null default mixed_case_monkeys_id.nextval primary key,
	"fleaCount" integer
)
\p\g
create sequence mixins_id;\p\g
create table mixins(
	id integer not null default mixins_id.nextval primary key,
	parent_id integer,
	pos integer,
	created_at ingresdate,
	updated_at ingresdate,
	lft integer,
	rgt integer,
	root_id integer,
	type varchar(255)
)
\p\g
create sequence moviesid;\p\g
create table movies(
	movieid integer not null default moviesid.nextval primary key,
	name varchar(255)
)
\p\g
create sequence non_poly_ones_id;\p\g
create table non_poly_ones(
	id integer not null default non_poly_ones_id.nextval primary key
)
\p\g
create sequence non_poly_twos_id;\p\g
create table non_poly_twos(
	id integer not null default non_poly_twos_id.nextval primary key
)
\p\g
create sequence numeric_data_id;\p\g
create table numeric_data(
	id integer not null default numeric_data_id.nextval primary key,
	bank_balance decimal(39,15),
	big_bank_balance decimal(39,15),
	world_population decimal(39,15),
	my_house_population decimal(39,15),
	decimal_number_with_default decimal(39,15) default 2.78,
	temperature float
)
\p\g
create sequence orders_id;\p\g
create table orders(
	id integer not null default orders_id.nextval primary key,
	name varchar(255),
	billing_customer_id integer,
	shipping_customer_id integer
)
\p\g
create sequence organizations_id;\p\g
create table organizations(
	id integer not null default organizations_id.nextval primary key,
	name varchar(255)
)
\p\g
create sequence owners_id;\p\g
create table owners(
	owner_id integer not null default owners_id.nextval primary key,
	name varchar(255),
	updated_at ingresdate,
	happy_at ingresdate
)
\p\g
create sequence paint_colors_id;\p\g
create table paint_colors(
	id integer not null default paint_colors_id.nextval primary key,
	non_poly_one_id integer
)
\p\g
create sequence paint_textures_id;\p\g
create table paint_textures(
	id integer not null default paint_textures_id.nextval primary key,
	non_poly_two_id integer
)
\p\g
create sequence parrots_id;\p\g
create table parrots(
	id integer not null default parrots_id.nextval primary key,
	name varchar(255),
	parrot_sti_class varchar(255),
	killer_id integer,
	created_at ingresdate,
	created_on ingresdate,
	updated_at ingresdate,
	updated_on ingresdate
)
\p\g
create table parrots_pirates(
	parrot_id integer,
	pirate_id integer
)
\p\g
create table parrots_treasures(
	parrot_id integer,
	treasure_id integer
)
\p\g
create sequence people_id;\p\g
create table people(
	id integer not null default people_id.nextval primary key,
	first_name varchar(255) not null,
	primary_contact_id integer,
	gender varchar(1),
	lock_version integer not null default '0'
)
\p\g
create sequence pets_id;\p\g
create table pets(
	pet_id integer not null default pets_id.nextval,
	name varchar(255),
	owner_id integer,
	integer integer
)
\p\g
create sequence pirates_id;\p\g
create table pirates(
	id integer not null default pirates_id.nextval primary key,
	catchphrase varchar(255),
	parrot_id integer,
	created_on ingresdate,
	updated_on ingresdate
)
\p\g
create sequence posts_id;\p\g
create table posts(
	id integer not null default posts_id.nextval primary key,
	author_id integer,
	title varchar(255) not null,
	body varchar(32000) not null,
	type varchar(255),
	comments_count integer default '0',
	taggings_count integer default '0'
)
\p\g
create sequence price_estimates_id;\p\g
create table price_estimates(
	id integer not null default price_estimates_id.nextval primary key,
	estimate_of_type varchar(255),
	estimate_of_id integer,
	price integer
)
\p\g
create sequence projects_id;\p\g
create table projects(
	id integer not null default projects_id.nextval primary key,
	name varchar(255),
	type varchar(255)
)
\p\g
create sequence readers_id;\p\g
create table readers(
	id integer not null default readers_id.nextval primary key,
	post_id integer not null,
	person_id integer not null
)
\p\g
create sequence references_id
\p\g
create table "references"(
	id integer not null default references_id.nextval primary key,
	person_id integer,
	job_id integer,
	favourite i1,
	lock_version integer default '0'
)
\p\g
create sequence shape_expressions_id
\p\g
create table shape_expressions(
	id integer not null default shape_expressions_id.nextval primary key,
	paint_type varchar(255),
	paint_id integer,
	shape_type varchar(255),
	shape_id integer
)
\p\g
create sequence ships_parts_id\p\g
create table ship_parts(
	id integer not null default ships_parts_id.nextval primary key,
	name varchar(255),
	ship_id integer
)
\p\g
create sequence ships_id;\p\g
create table ships(
	id integer not null default ships_id.nextval primary key,
	name varchar(255),
	pirate_id integer,
	created_at ingresdate,
	created_on ingresdate,
	updated_at ingresdate,
	updated_on ingresdate
)
\p\g
create sequence sponsors_id;\p\g
create table sponsors(
	id integer not null default sponsors_id.nextval primary key,
	club_id integer,
	sponsorable_id integer,
	sponsorable_type varchar(255)
)
\p\g
create sequence squares_id;\p\g
create table squares(
	id integer not null default squares_id.nextval primary key
)
\p\g
create table subscribers(
	nick varchar(255) not null,
	name varchar(255)
)
\p\g
create sequence subscriptions_id;\p\g
create table subscriptions(
	id integer not null default subscriptions_id.nextval primary key,
	subscriber_id varchar(255),
	book_id integer
)
\p\g
create sequence taggings_id;\p\g
create table taggings(
	id integer not null default taggings_id.nextval primary key,
	tag_id integer,
	super_tag_id integer,
	taggable_type varchar(255),
	taggable_id integer
)
\p\g
create sequence tags_id;\p\g
create table tags(
	id integer not null default tags_id.nextval primary key,
	name varchar(255),
	taggings_count integer default 0
)
\p\g
create sequence tasks_id;\p\g
create table tasks(
	id integer not null default tasks_id.nextval primary key,
	starting ingresdate,
	ending ingresdate
)
\p\g
create sequence topics_id;\p\g
create table topics(
	id integer not null default topics_id.nextval primary key,
	title varchar(255),
	author_name varchar(255),
	author_email_address varchar(255),
	written_on ingresdate,
	bonus_time time without time zone,
	last_read ansidate,
	content varchar(32000),
	approved i1 default 1,
	replies_count integer default '0',
	parent_id integer,
	parent_title varchar(255),
	type varchar(255)
)
\p\g
create sequence toys_id;\p\g
create table toys(
	toy_id integer not null default toys_id.nextval primary key,
	name varchar(255),
	pet_id integer,
	integer integer
)
\p\g
create sequence treasures_id;\p\g
create table treasures(
	id integer not null default treasures_id.nextval primary key,
	name varchar(255),
	looter_id integer,
	looter_type varchar(255)
)
\p\g
create sequence triangles_id;\p\g
create table triangles(
	id integer not null default triangles_id.nextval primary key
)
\p\g
create sequence vertices_id;\p\g
create table vertices(
	id integer not null default vertices_id.nextval primary key,
	label varchar(255)
)
\p\g
create sequence warehouse_things_id;\p\g
create table "warehouse-things"(
	id integer not null default warehouse_things_id.nextval primary key,
	value integer
)
\p\g
