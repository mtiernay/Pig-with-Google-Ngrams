-- This program counts the 4 words before and after any word used in Google's Ngrams
-- Note that there are two places below where you must place the word you want to study 

-- If you want to do this as an interactive session, SSH into amazon
--ssh -i '/key_location.pem' hadoop@ec2...rest of key


-- Load in the necessary SequenceFileLoader UDF for the compressed data
REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();



-- Load Data
data = LOAD '$INPUT' USING SequenceFileLoader AS (noidea:int, ngram:chararray);
-- If you are using an interactive session
--data = LOAD 's3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/5gram/data' USING SequenceFileLoader AS (noidea:int, ngram:chararray);


data1 = foreach data generate FLATTEN(STRSPLIT(ngram, '\t'));

data2 = foreach data1 generate FLATTEN(STRSPLIT($0, ' ')) AS (f1:chararray, f2:chararray, f3:chararray, f4:chararray, f5:chararray) , $1 AS year:int, $2 AS count:int;

data2a = foreach data2 generate LOWER(f1) AS f1, LOWER(f2) AS f2, LOWER(f3) AS f3, LOWER(f4) AS f4, LOWER(f5) AS f5, year, count;







---------------------------------------------------------------------------------------------------------------
-- Put the word of interest in the ' ' - this gives 4 words before
data3 = filter data2a by f5 matches '';

--  Count all the words after our word of interest
grouped = GROUP data3 BY (f2, year);
counts2 = FOREACH grouped GENERATE group, SUM(data3.count) AS count2;

grouped = GROUP data3 BY (f3, year);
counts3 = FOREACH grouped GENERATE group, SUM(data3.count) AS count3;

grouped = GROUP data3 BY (f4, year);
counts4 = FOREACH grouped GENERATE group, SUM(data3.count) AS count4;

grouped = GROUP data3 BY (f1, year);
counts5 = FOREACH grouped GENERATE group, SUM(data3.count) AS count5;












---------------------------------------------------------------------------------------------------------------
-- Put the word of interest in the ' ' - this gives 4 words after
data4 = filter data2a by f1 matches '';

--  Count all the words after our word of interest
grouped = GROUP data4 BY (f2, year);
counts6 = FOREACH grouped GENERATE group, SUM(data4.count) AS count6;

grouped = GROUP data4 BY (f3, year);
counts7 = FOREACH grouped GENERATE group, SUM(data4.count) AS count7;

grouped = GROUP data4 BY (f4, year);
counts8 = FOREACH grouped GENERATE group, SUM(data4.count) AS count8;

grouped = GROUP data4 BY (f5, year);
counts9 = FOREACH grouped GENERATE group, SUM(data4.count) AS count9;







---------------------------------------------------------------------------------------------------------------
-- Merge all the words together - This generates they word as a key, with each column as a bag and the total number of counts at that position
all_data = cogroup counts2 BY $0, counts3 BY $0, counts4 BY$0, counts5 BY$0, counts6 BY$0, counts7 BY$0, counts8 BY$0, counts9 BY$0;







---------------------------------------------------------------------------------------------------------------
-- Remove the bags (and make sure missing values don't screw everything up)
noempty = foreach all_data generate group, flatten(((counts2.count2 is null or IsEmpty(counts2.count2)) ? null : counts2.count2)),
 flatten(((counts3.count3 is null or IsEmpty(counts3.count3)) ? null : counts3.count3)),
 flatten(((counts4.count4 is null or IsEmpty(counts4.count4)) ? null : counts4.count4)),
 flatten(((counts5.count5 is null or IsEmpty(counts5.count5)) ? null : counts5.count5)),
 flatten(((counts6.count6 is null or IsEmpty(counts6.count6)) ? null : counts6.count6)),
 flatten(((counts7.count7 is null or IsEmpty(counts7.count7)) ? null : counts7.count7)),
 flatten(((counts8.count8 is null or IsEmpty(counts8.count8)) ? null : counts8.count8)),
 flatten(((counts9.count9 is null or IsEmpty(counts9.count9)) ? null : counts9.count9));

-- Turn the nulls into zeros so that pig can add them
noempty2 = foreach noempty generate group, (($1 is null) ? 0 : $1), (($2 is null) ? 0 : $2), (($3 is null) ? 0 : $3), (($4 is null) ? 0 : $4),
 (($5 is null) ? 0 : $5), (($6 is null) ? 0 : $6), (($7 is null) ? 0 : $7), (($8 is null) ? 0 : $8); 





---------------------------------------------------------------------------------------------------------------
-- This gives ((word,year),number of times word directly after, number of times 2nd after, 3rd after, 4th after)
summed = foreach noempty2 generate flatten(group) AS (z1,z2), $1+$2+$3+$4+$5+$6+$7+$8 AS summed;


---------------------------------------------------------------------------------------------------------------
-- Sort by number of summed references
grouped = GROUP summed by z1;
total_ordered = FOREACH grouped GENERATE group AS names, SUM(summed.summed) AS totals;


-- Merge in totals summed
merge1 = JOIN total_ordered BY names, summed BY z1;
merge2 = FOREACH merge1 GENERATE names, totals, z2, summed;

---------------------------------------------------------------------------------------------------------------
-- This gives us (word, total number of times used, year, number of times used that year)
final_output = ORDER merge2 BY totals DESC, names, z2;





---------------------------------------------------------------------------------------------------------------
store final_output INTO '$OUTPUT';
-- If using an interactive session
--store final_output INTO 's3://...';




