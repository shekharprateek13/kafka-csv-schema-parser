package csv.schema.parser.service;

import csv.schema.parser.constants.SchemaDataTypes;
import csv.schema.parser.task.CsvParserTask;
import csv.schema.parser.util.CsvParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CsvParserService {

    private static Logger logger = LoggerFactory.getLogger(CsvParserService.class);

    public Map<String,String> getCSVSchema(BufferedReader bReader,boolean hasHeader, String token, int numThreads, int chunkSize){
        ExecutorService csvParser = new ThreadPoolExecutor(numThreads,numThreads,1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(numThreads, true),
                new ThreadPoolExecutor.CallerRunsPolicy());

        List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();
        List<Map<String,Set<String>>> schemaMapList = new ArrayList<>();
        List<String> tempList = new ArrayList<>();
        int numberOfLinesRead = 0;
        String tempStr = "";

        try{
            String header = bReader.readLine();
            Map<String,String> defaultSchemaMap = hasHeader?CsvParserUtil.getColumnNamesMap(header,token):CsvParserUtil.getDefaultColumnNamesMap(header,token);

            if(!hasHeader){
                tempList.add(tempStr);
                numberOfLinesRead++;
            }

            while((tempStr = bReader.readLine()) != null){
                tempList.add(tempStr);
                numberOfLinesRead++;

                if(numberOfLinesRead % chunkSize == 0) {
                    futures.add(csvParser.submit(new CsvParserTask(tempList,defaultSchemaMap)));
                    tempList = new ArrayList<>();
                }
            }
            futures.add(csvParser.submit(new CsvParserTask(tempList,defaultSchemaMap)));

            csvParser.shutdown();
            csvParser.awaitTermination(1,TimeUnit.DAYS);

            logger.info("Finished parsing the file");
            logger.debug("Starting to combine the output of different threads");
            logger.info("Printing Generated Schema Maps List from different threads");

            for(int i = 0; i < futures.size();i++){
                Map<String,Set<String>> tempMap = futures.get(i).get();
                schemaMapList.add(tempMap);
                logger.debug(tempMap.toString());
            }
            bReader.close();
        }catch (FileNotFoundException e){
            logger.error("Cannot find the CSV File at the given location: "+e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        }

        return mergeSchemaMaps(schemaMapList);
    }

    public Map<String,String> getCSVSchema(String filePath,boolean hasHeader, String token, int numThreads, int chunkSize){
        logger.info("Starting to parse the file at path: "+filePath);
        ExecutorService csvParser = new ThreadPoolExecutor(numThreads,numThreads,1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(numThreads, true),
                new ThreadPoolExecutor.CallerRunsPolicy());

        List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();
        List<Map<String,Set<String>>> schemaMapList = new ArrayList<>();
        List<String> tempList = new ArrayList<>();
        File file = new File(filePath);
        int numberOfLinesRead = 0;
        String tempStr = "";

        try{
            FileReader fileReader = new FileReader(file);
            BufferedReader bReader = new BufferedReader(fileReader);
            String header = bReader.readLine();
            Map<String,String> defaultSchemaMap = hasHeader?CsvParserUtil.getColumnNamesMap(header,token):CsvParserUtil.getDefaultColumnNamesMap(header,token);

            if(!hasHeader){
                tempList.add(tempStr);
                numberOfLinesRead++;
            }

            while((tempStr = bReader.readLine()) != null){
                tempList.add(tempStr);
                numberOfLinesRead++;

                if(numberOfLinesRead % chunkSize == 0) {
                    futures.add(csvParser.submit(new CsvParserTask(tempList,defaultSchemaMap)));
                    tempList = new ArrayList<>();
                }
            }
            futures.add(csvParser.submit(new CsvParserTask(tempList,defaultSchemaMap)));

            csvParser.shutdown();
            csvParser.awaitTermination(1,TimeUnit.DAYS);

            logger.info("Finished parsing the file at path: "+filePath);
            logger.debug("Starting to combine the output of different threads");
            logger.info("Printing Generated Schema Maps List from different threads");

            for(int i = 0; i < futures.size();i++){
                Map<String,Set<String>> tempMap = futures.get(i).get();
                schemaMapList.add(tempMap);
                logger.debug(tempMap.toString());
            }
            bReader.close();
            fileReader.close();
        }catch (FileNotFoundException e){
           logger.error("Cannot find the CSV File at the given location: "+e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        }

        return mergeSchemaMaps(schemaMapList);
    }

    private Map<String,String> mergeSchemaMaps(List<Map<String, Set<String>>> schemaMapList){
        if(schemaMapList.size() == 0) return null;

        Map<String,Set<String>> tempMap = new LinkedHashMap<>();
        for(Map<String,Set<String>> map: schemaMapList){
            for(String key: map.keySet()){
                if(tempMap.containsKey(key)){
                    Set<String> tempSet = tempMap.get(key);
                    tempSet.addAll(map.get(key));
                    tempMap.put(key,tempSet);
                }else{
                    tempMap.put(key,map.get(key));
                }
            }
        }

        Map<String,String> mergedSchemaMap = new LinkedHashMap<>();
        for(String key: tempMap.keySet()){
            Set<String> tempSet = tempMap.get(key);
            String dataType = "";
            if(tempSet.contains(SchemaDataTypes.STRING.toString())){
                dataType = SchemaDataTypes.STRING.toString();
            }else if(tempSet.contains(SchemaDataTypes.FLOAT64.toString())){
                dataType = SchemaDataTypes.FLOAT64.toString();
            }else {
                dataType = SchemaDataTypes.INT64.toString();
            }
            mergedSchemaMap.put(key,dataType);
        }
        return mergedSchemaMap;
    }
}