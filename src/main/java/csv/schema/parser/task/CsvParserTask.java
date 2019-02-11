package csv.schema.parser.task;

import csv.schema.parser.util.CsvParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class CsvParserTask implements Callable<Map<String, Set<String>>>{

    private static Logger logger = LoggerFactory.getLogger(CsvParserTask.class.getName());
    private List<String> csvList;
    private Map<String,String> schemaMap;

    public CsvParserTask(List<String> list,Map<String,String> schemaMap){
        this.csvList = list;
        this.schemaMap = new LinkedHashMap<>();
        this.schemaMap.putAll(schemaMap);
    }

    public Map<String, Set<String>> call() {
        List<List<String>> inferredDataTypes = csvList.stream().map(str -> {
            String[] temp = str.split(",");
            List<String> tempList = Arrays.stream(temp).map(data -> CsvParserUtil.getDataType(data)).collect(Collectors.toCollection(LinkedList::new));
            return tempList;
        }).collect(Collectors.toList());

        List<String> columnNames = new LinkedList<>();
        columnNames.addAll(schemaMap.keySet());

        Map<String, Set<String>> mergedSchemaMap = new LinkedHashMap<>();
        columnNames.stream().forEach(columnName -> mergedSchemaMap.put(columnName,new HashSet<String>()));

        for(int i = 0; i < inferredDataTypes.size();i++){
            List<String> recordRow = inferredDataTypes.get(i);
            try {
                for (int j = 0; j < recordRow.size(); j++) {
                    mergedSchemaMap.get(columnNames.get(j)).add(recordRow.get(j));
                }
            }catch(Exception e){
                logger.error("Missed Record Row:" + csvList.get(i));
                e.printStackTrace();
            }
        }
        return mergedSchemaMap;
    }
}
