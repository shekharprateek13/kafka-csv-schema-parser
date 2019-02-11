# kafka-csv-schema-parser
This project contains multi-threaded implementation of CSV schema parser for Kafka Data types. 

The current supported data types are STRING, INT64 and FLOAT64. 

The main class to be called is 'CsvParser' and it contains overloaded methods 'parseCSV'. For using this class, just clone the repo and add it to your gradle/maven/nexus repository. Instantiate a new class of 'CsvParser' and call the methods as per the requirement.

The code could be easily modified to support other data types for Kafka or in general. Just modify the 'getDataTye(String)' function in CsvParserUtil.java file. Also, add a matcher for the corresponding data type you want to add.
