package csv.schema.parser.constants;

public enum SchemaDataTypes {
    /**
     *  64-bit signed integer
     */
    INT64,
    /**
     *  64-bit IEEE 754 floating point number
     */
    FLOAT64,
    /**
     * Boolean value (true or false)
     */
    BOOLEAN,
    /**
     * Character string that supports all Unicode characters.
     *
     * Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory representation.
     */
    STRING
}
