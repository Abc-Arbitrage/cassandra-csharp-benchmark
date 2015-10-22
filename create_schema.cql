CREATE KEYSPACE "CsharpDriverBenchmark"
WITH replication = {'class': 'NetworkTopologyStrategy', '<INSERT_YOUR_DC_HERE>': 2 };

USE "CsharpDriverBenchmark";

CREATE TABLE "Timeserie" (
	"SerieId" uuid,
	"Day" timestamp,
	
	"UtcDate" timestamp,
	"Value" double,
	
	PRIMARY KEY (("SerieId", "Day"), "UtcDate")
);

CREATE TABLE "SerieId" (
	"SerieId" uuid,
	PRIMARY KEY ("SerieId")
);