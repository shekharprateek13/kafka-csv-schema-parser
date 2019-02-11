package csv.schema.parser.util;

import csv.schema.parser.constants.SchemaDataTypes;
import static csv.schema.parser.constants.Constants.DEFAULT_COLUMN_NAME;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class CsvParserUtil {

    public static Pattern integerPattern = Pattern.compile("[+-]?[0-9][0-9]*");
    public static Pattern floatingPointPattern = Pattern.compile("^([+-]?\\d*\\.?\\d*)$");

    public static Map<String,String> getColumnNamesMap(String csvHeader,String token){
        Map<String,String> map = new LinkedHashMap<>();

        for(String columnName: csvHeader.split(token)){
            map.put(columnName,"NONE");
        }
        return map;
    }

    public static Map<String,String> getDefaultColumnNamesMap(String csvHeader,String token){
        Map<String,String> map = new LinkedHashMap<>();

        String columns[] = csvHeader.split(token);
        IntStream.range(0, columns.length).forEach(index -> {
            String columnName =  DEFAULT_COLUMN_NAME + "_" + ++index;
            map.put(columnName,"NONE");
        });

        return map;
    }

    public static String getDataType(String inputData){

        Matcher matcher = integerPattern.matcher(inputData);
        if(matcher.matches()) return SchemaDataTypes.INT64.toString();

        matcher = floatingPointPattern.matcher(inputData);
        if(matcher.matches()) return SchemaDataTypes.FLOAT64.toString();

        return SchemaDataTypes.STRING.toString();
    }
}
