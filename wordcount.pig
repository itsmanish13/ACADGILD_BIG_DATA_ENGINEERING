FileData = LOAD '/home/acadgild/manish/news.txt' AS(line:Chararray);
words = FOREACH FileData GENERATE FLATTEN(TOKENIZE(line,' ')) as word; 
wordGroup = GROUP words BY word; 
wordCount = FOREACH wordGroup GENERATE group, COUNT(words);  
wordCountSorted = ORDER wordCount by $1 desc;
DUMP wordCountSorted;