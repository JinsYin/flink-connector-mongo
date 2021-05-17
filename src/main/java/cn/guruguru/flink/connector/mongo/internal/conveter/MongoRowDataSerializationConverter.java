package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
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
 * Class for converting {@link RowData} to {@link BsonDocument}
 */
public class MongoRowDataSerializationConverter implements MgSerializationConverter<RowData> {

    // ~ instance fields -------------------------------------------

    private final RowType rowType;
    private final MongoSerializationSetter[] toExternalSetters;   // a set of serialization functions

    // ~ constructor ------------------------------------------------

    public MongoRowDataSerializationConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.toExternalSetters = new MongoSerializationSetter[rowType.getFieldCount()];

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalSetters[i] = createNullableExternalSetter(rowType.getTypeAt(i), fieldNames.get(i));
        }
    }

    // ~ methods ---------------------------------------------------

    @Override
    public BsonDocument toExternal(RowData rowData) throws MongoTypeConversionException {
        BsonDocument bsonDocument = new BsonDocument();
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalSetters[index].set(bsonDocument, index, rowData);
        }
        return bsonDocument;
    }

    @Override
    public BsonDocument toExternal(RowData rowData, BsonDocument bsonDocument) throws MongoTypeConversionException {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalSetters[index].set(bsonDocument, index, rowData);
        }
        return bsonDocument;
    }

    // --------------------------------------------------------------
    // Function interfaces
    // --------------------------------------------------------------

    @FunctionalInterface
    private interface MongoSerializationSetter extends Serializable {
        void set(BsonDocument doc, int pos, RowData row); // doc.put(fieldNames[pos], row.getField(pos))
    }

    @FunctionalInterface
    private interface MongoSerializationConverter extends Serializable {
        BsonValue serialize(Object value); // Object: RowData, StringData ...
    }

    // --------------------------------------------------------------
    // Serialization setters : doc.put(fieldName, row.getField(pos))
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
                setter.set(doc, pos, row);
            }
        };
    }

    /**
     * doc.put(fieldName, row.getField(pos))
     * @see GenericRowData#getDecimal(int, int, int)
     * @see BinaryRowData#getDecimal(int, int, int)
     *
     * 注：SQL 的 ROW.ROW 类型转换成了内部的 BinaryRowData（最外层的 ROW 也是？），但测试时往往使用了 GenericRowData 而忽略了该问题
     */
    private MongoSerializationSetter createNotNullExternalSetter(LogicalType type, String fieldName) {
        return (doc, pos, row) -> {
            Object value;
            // `row` may be a BinaryRowData or a GenericRowData
            if (row instanceof BinaryRowData) {
                // 更通用，但如果 ROW.ROW 是 GenericRowData 会出现如下异常（ROW.ROW 一定是 BinaryRowData？）
                // java.math.BigDecimal cannot be cast to org.apache.flink.table.data.DecimalData
                value = RowData.createFieldGetter(type, pos).getFieldOrNull(row);
            } else {
                // 如果 ROW 或 ROW.ROW 是 BinaryRowData 会出现如下异常：
                // BinaryRowData cannot be cast to org.apache.flink.table.data.GenericRowData
                value = ((GenericRowData) row).getField(pos);
            }
            doc.append(fieldName, createNullableExternalConverter(type).serialize(value));
        };
    }

    // --------------------------------------------------------------
    // Serialization converters (StringData/ArrayData../ -> BsonValue)
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
                return value -> new BsonBoolean((boolean) value); // boolean -> BsonBoolean
            case DOUBLE:
                return value -> new BsonDouble((double) value); // double -> BsonDouble
            case BINARY:
            case VARBINARY:
                return value -> new BsonBinary((byte[]) value); // byte[] -> BsonBinary
            case TINYINT:
                return value -> new BsonBinary(new byte[]{(byte) value}); // byte -> BsonBinary
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> new BsonInt32((int) value); // int -> BsonInt32
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> new BsonInt64((long) value); // long -> BsonInt64
            case DATE:
                return createExternalDateConverter((DateType) type);
            case TIME_WITHOUT_TIME_ZONE:
                return createExternalTimeConverter((TimeType) type);
            //case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createExternalTimestampConverter((TimestampType) type); // TimestampType -> BsonDateTime
            case CHAR:
            case VARCHAR:
                return value -> {
                    StringData stringData = (StringData) value;
                    return new BsonString(stringData.toString()); // StringData -> BsonString
                };
            case ROW:
                return createExternalRowConverter((RowType) type); // RowData -> BsonDocument
            case MAP:
            case MULTISET:
                return createExternalMapConverter((MapType) type); // MapData -> BsonDocument
            case ARRAY:
                return createExternalArrayConverter((ArrayType) type); // ArrayData -> BsonArray
            case DECIMAL:
                return createExternalDecimalConverter((DecimalType) type); // DecimalData -> BsonDecimal128
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
     * <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/types.html#date">Date</a>
     */
    private MongoSerializationConverter createExternalDateConverter(DateType dateType) {
        return value -> {
            // way 1: int -> BsonDateTime
            int days = (int) value; // `(int) value` describes the number of days since epoch.
            return new BsonDateTime(TimeUnit.DAYS.toMillis(days));

            // way 2: LocalDate -> BsonDateTime
            //LocalDate localDate = (LocalDate) value;
            //long milliseconds = localDate.atTime(LocalTime.MIDNIGHT)
            //        .atZone(ZoneId.systemDefault())
            //        .toInstant()
            //        .toEpochMilli();
            //return new BsonDateTime(milliseconds);
        };
    }

    /**
     * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/types.html#time">Time</a>
     *
     */
    private MongoSerializationConverter createExternalTimeConverter(TimeType type) {
        return value -> {
            // way 1: int -> BsonDateTime
            long milliseconds = (int) value; // `(int) value` describes the number of milliseconds of the day.
            return new BsonDateTime(milliseconds);

            // way 2: LocalTime -> BsonDateTime
            //LocalTime localTime = (LocalTime) value;
            //long milliseconds = localTime.atDate(LocalDate.MAX)
            //        .atZone(ZoneId.systemDefault())
            //        .toInstant().toEpochMilli();
            //return new BsonDateTime(milliseconds);
        };
    }

    /**
     * value -> new DataTime()
     *
     * see org.apache.flink.table.data.TimestampDataTest
     *
     * @see <a href="https://stackoverflow.com/questions/57487414/how-to-understand-bson-timestamp"></a>
     * @see <a href="https://pymongo.readthedocs.io/en/stable/api/bson/timestamp.html"></a>
     */
    private MongoSerializationConverter createExternalTimestampConverter(TimestampType tsType) {
        return (Object value) -> {
            TimestampData tsData = (TimestampData) value;
            long milliseconds = tsData.getMillisecond();

            // use with the MongoDB opLog
            //long seconds = TimeUnit.MILLISECONDS.toSeconds(milliseconds);
            //int increment = 0;
            //long tsValue = seconds << 32 | (increment & 0xFFFFFFFFL);
            //return new BsonTimestamp(tsValue);

            return new BsonDateTime(milliseconds);
        };
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
                fieldSetters.get(i).set(doc, i, row);
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
