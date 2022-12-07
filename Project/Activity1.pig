inputFile = LOAD 'hdfs:///user/ankita/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray,line:chararray);
inputFile = LOAD 'hdfs:///user/ankita/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray,line:chararray);
inputFile = LOAD 'hdfs:///user/ankita/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray,line:chararray);
ranked = RANK inputFile;
Dialouges = FILTER ranked BY (rank_inputFile > 2);
groupByName = GROUP Dialouges BY name;

names = FOREACH groupByName GENERATE $0 as name,COUNT($1) as no_of_lines;
namesOrder = ORDER names BY no_of_lines DESC;

STORE namesOrder INTO 'hdfs:///user/ankita/results/Activity1Output' USING PigStorage('\t');
