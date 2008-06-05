/* Database: cookbook */

/* Table structure for table categories */

DROP TABLE "categories";\p\g
CREATE TABLE "categories" (
  "id" int NOT NULL,
  "name" varchar(50) default NULL,
  PRIMARY KEY  ("id")
);\p\g

/* Loading data for table categories */

INSERT INTO "categories" VALUES (1,'Snacks');\p\g
INSERT INTO "categories" VALUES (2,'Beverages');\p\g


/* Table structure for table recipes */

DROP TABLE "recipes";\p\g
CREATE TABLE "recipes" (
  "id" int NOT NULL,
  "title" varchar(255) NOT NULL default '',
  "description" varchar(255) default NULL,
  "date" date default NULL,
  "instructions" text(2000),
  "category_id" int default NULL,
  PRIMARY KEY  ("id")
);\p\g

/* Loading data for table recipes */

INSERT INTO "recipes" VALUES (1,'Hot Chips','Only for the brave!','2004-11-11','    Sprinkle hot-pepper sauce on corn chips.\r\n  ',1);\p\g
INSERT INTO "recipes" VALUES (2,'Ice Water','Everyone''s favorite.','2004-11-11','Put ice cubes in a glass of water.\r\n  ',2);\p\g
