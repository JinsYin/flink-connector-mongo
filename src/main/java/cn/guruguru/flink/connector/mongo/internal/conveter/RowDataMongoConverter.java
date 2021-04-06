package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.bson.*;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * see org.apache.flink.formats.json.JsonRowDataDeserializationSchema
 * see org.apache.flink.formats.json.JsonRowDataSerializationSchema
 * see org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter
 *
 * @see <a href="https://docs.mongodb.com/manual/reference/bson-types/">BSON Types</a>
 */
public class RowDataMongoConverter implements
        MongoDeserializationConverter<RowData>,
        MongoSerializationConverter<RowData> {

    // ~ instance fields -------------------------------------------

    private final RowType rowType;
    private final MongoSerializationSetter[] toExternalConverters;   // a set of serialization functions
    private final MongoDeserializationConverter[] toInternalConverters; // a set of deserialization functions

    // ~ constructor ------------------------------------------------

    public RowDataMongoConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.toExternalConverters = new MongoSerializationSetter[rowType.getFieldCount()];
        this.toInternalConverters = new MongoDeserializationConverter[rowType.getFieldCount()];

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters[i] = createNullableExternalSetter(rowType.getTypeAt(i), fieldNames.get(i));
            toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
    }

    // ~ methods -----------------------------------------------------

    @Override
    public BsonDocument toExternal(RowData rowData, BsonDocument bsonDocument) throws MongoTypeConversionException {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(bsonDocument, index, rowData);
        }
        return bsonDocument;
    }

    @Override
    public RowData toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        List<String> fieldNames = rowType.getFieldNames();
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            BsonValue bsonValue = bsonDocument.get(fieldNames.get(pos));
            genericRowData.setField(pos, toInternalConverters[pos].deserialize(bsonValue));
        }
        return genericRowData;
    }

    // --------------------------------------------------------------
    // Function interfaces
    // --------------------------------------------------------------

    @FunctionalInterface
    private interface MongoSerializationSetter extends Serializable {
        void serialize(BsonDocument doc, int pos, RowData row);
    }

    @FunctionalInterface
    private interface MongoSerializationConverter extends Serializable {
        BsonValue serialize(Object value);
    }

    @FunctionalInterface
    private interface MongoDeserializationConverter extends Serializable {
        Object deserialize(BsonValue value);
    }

    // --------------------------------------------------------------
    // Deserialization converters
    // --------------------------------------------------------------

    private MongoDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createNotNullInternalConverter(type));
    }

    private MongoDeserializationConverter wrapIntoNullableInternalConverter(
            MongoDeserializationConverter converter) {
        return bsonValue -> {
            if (bsonValue == null || BsonType.NULL.equals(bsonValue.getBsonType())) {
                return null;
            }
            return converter.deserialize(bsonValue);
        };
    }

    /**
     * @see BsonType
     */
    private MongoDeserializationConverter createNotNullInternalConverter(BsonValue bsonValue) {
        switch (bsonValue.getBsonType()) {
//            case DOUBLE:
//                return value -> ((BsonDouble) value).getValue();
//            case STRING:
//                return value -> ((BsonString) value).getValue();
//            case DOCUMENT:
//                return value -> createInternalDocumentConverter((BsonDocument) value);
//            case ARRAY:
//                return value -> createInternalArrayConverter((BsonArray) value);
//            case BINARY:
//                return value -> ((BsonBinary) value).getData();
//            case BOOLEAN:
//                return value -> ((BsonBoolean) value).getValue();
//            case DATE_TIME:
//                //
//            case INT32:
//                return value -> ((BsonInt32) value).getValue();
//            case INT64:
//                return value -> ((BsonInt64) value).getValue();
//            case TIMESTAMP:
//                return value -> TimestampData.fromEpochMillis(((BsonTimestamp) value).getValue());
//            case DECIMAL128:
//                return createInternalDecimal128Converter();
            default:
                throw new UnsupportedOperationException("Unsupported BsonValue type:" + bsonValue);
        }
    }

    private MongoDeserializationConverter createNotNullInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value -> ((BsonBoolean) value).getValue();
            case DOUBLE:
                return value -> ((BsonDouble) value).getValue();
            case BINARY:
            case VARBINARY:
                return value -> ((BsonBinary) value).getData();
            case TINYINT:
                return value -> ((BsonBinary) value).getData();
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> ((BsonInt32) value).getValue(); // BsonValue -> int
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> ((BsonInt64) value).getValue(); // BsonValue -> long
            case DATE:
                return value -> {
                    long milliseconds = ((BsonDateTime) value).getValue();
                    return TimeUnit.MILLISECONDS.toDays(milliseconds); // milliseconds -> days
                };
            case TIME_WITHOUT_TIME_ZONE:
                return value -> ((BsonDateTime) value).getValue(); // BsonValue -> long (milliseconds)
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value -> ((BsonTimestamp) value).getValue();
            case CHAR:
            case VARCHAR:
                return value -> ((BsonString) value).getValue(); // BsonValue -> String
            case ROW:
                return createInternalRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createInternalMapConverter((MapType) type);
            case ARRAY:
                return createInternalArrayConverter(((ArrayType) type));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private MongoDeserializationConverter createInternalMapConverter(MapType mapType) {
        return null;
    }

    private MongoDeserializationConverter createInternalRowConverter(RowType rowType) {
        return null;
    }

    private MongoDeserializationConverter createInternalArrayConverter(ArrayType arrayType) {
        return null;
    }

    private MongoDeserializationConverter createInternalDecimal128Converter() {
        return value -> {
            BsonDecimal128 bsonDecimal128 = ((BsonDecimal128) value);
            Decimal128 decimal128 = bsonDecimal128.decimal128Value();
            BigDecimal bigDecimal = decimal128.bigDecimalValue();
            int precision = bigDecimal.precision();
            int scale = bigDecimal.scale();
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    // --------------------------------------------------------------
    // Serialization setters
    // --------------------------------------------------------------

    private MongoSerializationSetter createNullableExternalSetter(LogicalType type, String fieldName) {
        return wrapIntoNullableExternalSetter(createNotNullExternalSetter(type, fieldName), type, fieldName);
    }

    private MongoSerializationSetter wrapIntoNullableExternalSetter(
            MongoSerializationSetter setter, LogicalType type, String fieldName) {
        return (doc, pos, row) -> {
            if (row == null || row.isNullAt(pos) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                doc.append(fieldName, new BsonNull());
            } else {
                setter.serialize(doc, pos, row);
            }
        };
    }

    private MongoSerializationSetter createNotNullExternalSetter(LogicalType type, String fieldName) {
        return (doc, pos, row) -> {
            Object value = ((GenericRowData) row).getField(pos);
            doc.append(fieldName, createNullableExternalConverter(type).serialize(value));
        };
    }

    // --------------------------------------------------------------
    // Serialization converters
    // --------------------------------------------------------------

    private MongoSerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createNotNullExternalConverter(type), type);
    }

    private MongoSerializationConverter wrapIntoNullableExternalConverter(
            MongoSerializationConverter converter, LogicalType type) {
        return value -> {
            if (value == null || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                return new BsonNull();
            } else {
                return converter.serialize(value);
            }
        };
    }

    private MongoSerializationConverter createNotNullExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value -> new BsonBoolean((boolean) value);
            case DOUBLE:
                return value -> new BsonDouble((double) value);
            case BINARY:
            case VARBINARY:
                return value -> new BsonBinary((byte[]) value);
            case TINYINT:
                return value -> new BsonBinary(new byte[]{(byte) value});
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> new BsonInt32((int) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> new BsonInt64((long) value);
            case DATE:
                return value -> {
                    // Describes the number of days since epoch.
                    int days = (int) value;
                    long milliseconds = TimeUnit.DAYS.toMillis(days);
                    return new BsonDateTime(milliseconds);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return value -> {
                    // Describes the number of milliseconds of the day.
                    int milliseconds = (int) value;
                    return new BsonDateTime(milliseconds);
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value -> {
                    final int precision = ((TimestampType) type).getPrecision();
                    TimestampData tsData = (TimestampData) value;
                    long milliseconds = tsData.getMillisecond();
                    return new BsonTimestamp(milliseconds);
                };
            case CHAR:
            case VARCHAR:
                return value -> {
                    StringData stringData = (StringData) value;
                    return new BsonString(stringData.toString());
                };
            case ROW:
                return createExternalRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createExternalMapConverter((MapType) type);
            case ARRAY:
                return createExternalArrayConverter((ArrayType) type);
            case DECIMAL:
                return createExternalDecimalConverter((DecimalType) type);
            case SMALLINT:
            case FLOAT:
            case SYMBOL: // BsonSymbol
            case RAW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * value -> new BsonDecimal128(new Decimal128(BigDecimal))
     *
     * see org.apache.flink.table.data.DecimalDataTest
     */
    private MongoSerializationConverter createExternalDecimalConverter(DecimalType decimalType) {
        return (Object value) -> {
            final int precision = decimalType.getPrecision(); // schema precision
            final int scale = decimalType.getScale(); // schema scale

            DecimalData decimalData = value instanceof BigInteger ?
                    DecimalData.fromBigDecimal(new BigDecimal((BigInteger) value, 0), precision, scale) :
                    DecimalData.fromBigDecimal((BigDecimal) value, precision, scale);

            // precision and scale from data
            // decimalData.precision();
            // decimalData.scale();

            BigDecimal bigDecimal = decimalData.toBigDecimal();
            return new BsonDecimal128(new Decimal128(bigDecimal));
        };
    }

    /**
     * value -> new BsonDocument(List<BsonValue>)
     */
    private MongoSerializationConverter createExternalRowConverter(RowType rowType) {
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);

        final List<MongoSerializationSetter> fieldSetters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldSetters.add(createNullableExternalSetter(fieldTypes[i], fieldNames[i]));
        }

        return (Object value) -> {
            RowData row = (RowData) value;
            BsonDocument doc = new BsonDocument();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                fieldSetters.get(i).serialize(doc, i, row);
            }
            return doc;
        };
    }

    /**
     * value -> new BsonArray(List<BsonValue>)
     */
    private MongoSerializationConverter createExternalArrayConverter(ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        final MongoSerializationConverter elementConverter = createNotNullExternalConverter(elementType);

        return (Object value) -> {
            ArrayData arrayData = (ArrayData) value;
            List<BsonValue> bsonValues = new ArrayList<>();
            for (int i = 0; i < arrayData.size(); i++) {
                Object element = ArrayData.createElementGetter(elementType).getElementOrNull(arrayData, i);
                BsonValue bsonValue = elementConverter.serialize(element);
                bsonValues.add(bsonValue);
            }
            return new BsonArray(bsonValues);
        };
    }

    /**
     * value -> new BsonDocument(List<BsonElement>)
     */
    private MongoSerializationConverter createExternalMapConverter(MapType mapType) {
        final LogicalType keyType = mapType.getKeyType();
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "MongoDB Connector doesn't support non-string as key type of map. " +
                            "The map type is: " + mapType.asSummaryString());
        }
        final LogicalType valType = mapType.getValueType();
        final MongoSerializationConverter valConverter = createNullableExternalConverter(valType);

        return (Object value) -> {
            MapData mapData = (MapData) value;

            ArrayData keyArray = mapData.keyArray();
            ArrayData valArray = mapData.valueArray();

            List<BsonElement> bsonElements = new ArrayList<>();
            for (int i = 0; i < mapData.size(); i++) {
                String key = keyArray.getString(i).toString(); // key must be string
                Object val = ArrayData.createElementGetter(valType).getElementOrNull(valArray, i);
                BsonElement bsonElement = new BsonElement(key, valConverter.serialize(val));
                bsonElements.add(bsonElement);
            }
            return new BsonDocument(bsonElements);
        };
    }
}
