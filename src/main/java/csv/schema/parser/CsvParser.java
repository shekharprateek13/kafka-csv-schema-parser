package csv.schema.parser;

import csv.schema.parser.service.CsvParserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.Map;

import static csv.schema.parser.constants.Constants.DEFAULT_CHUNK_SIZE;
import static csv.schema.parser.constants.Constants.DEFAULT_NUMBER_OF_THREADS;
import static csv.schema.parser.constants.Constants.DEFAULT_TOKEN;

public class CsvParser {

    private static Logger logger = LoggerFactory.getLogger(CsvParser.class);

    /**
     * This function returns the schema of input CSV. The returned types are from the following sets of data types:
     * INT64   -> INTEGER 64 Byte
     * FLOAT64 -> FLOAT 64 Byte
     * STRING  -> STRING

     * @param filePath: the file path of the input CSV.
     * @param numThreads: the number of threads which will be used to parse the csv. Default Value - 4
     * @param chunkSize: the chunk size (number of lines) which each thread will parse. Default Value - 25000
     * @return A Map of Key Value pairs which contain the key as the column name of the csv and corresponding value as the data type.
     */
    public Map<String,String> parseCSV(String filePath, boolean hasHeader, String token, int numThreads, int chunkSize){
        CsvParserService csvParserService = new CsvParserService();
        Map<String,String> schemaMap = null;

        numThreads = (numThreads <= 0)?DEFAULT_NUMBER_OF_THREADS:numThreads;
        chunkSize = (chunkSize <= 0)?DEFAULT_CHUNK_SIZE:chunkSize;
        token = token == null || token.isEmpty()?DEFAULT_TOKEN:token;

        try {
            schemaMap = csvParserService.getCSVSchema(filePath,hasHeader,token,numThreads,chunkSize);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.info("Generated Schema: "+schemaMap);
        return schemaMap;
    }

    public Map<String,String> parseCSV(BufferedReader bReader, boolean hasHeader, String token, int numThreads, int chunkSize){
        CsvParserService csvParserService = new CsvParserService();
        Map<String,String> schemaMap = null;

        numThreads = (numThreads <= 0)?DEFAULT_NUMBER_OF_THREADS:numThreads;
        chunkSize = (chunkSize <= 0)?DEFAULT_CHUNK_SIZE:chunkSize;
        token = token == null || token.isEmpty()?DEFAULT_TOKEN:token;

        try {
            schemaMap = csvParserService.getCSVSchema(bReader,hasHeader,token,numThreads,chunkSize);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.info("Generated Schema: "+schemaMap);
        return schemaMap;
    }
}
