package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataDeserializationConverter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoTypeConversionException;
import cn.guruguru.flink.connector.mongo.internal.options.MongoLookupOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoReadOptions;
import cn.guruguru.flink.connector.mongo.source.MongoRowDataLookupFunction;
import cn.guruguru.flink.connector.mongo.source.MongoRowDataSourceFunction;
import com.mongodb.client.model.geojson.*;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mongodb dynamic table for reading and joining.
 *
 * <p>The LookupTableSource and ScanTableSource extend {@link DynamicTableSource}
 *
 * @see org.apache.flink.table.factories.DataGenTableSourceFactory.DataGenTableSource
 * @see org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource
 * @author JinsYin
 */
public class MongoDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    public static final String GEO_TYPE_FIELD = "type";
    public static final String GEO_COORDINATES_FIELD = "coordinates";

    private final MongoOptions mongoOptions;
    private final MongoReadOptions readOptions;
    private final MongoLookupOptions lookupOptions;

    private transient final TableSchema tableSchema; // TableSchema is not serializable

    public MongoDynamicTableSource(
            TableSchema tableSchema,
            MongoOptions mongoOptions,
            MongoReadOptions readOptions,
            MongoLookupOptions lookupOptions) {
        this.tableSchema = tableSchema;
        this.mongoOptions = mongoOptions;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
    }

    // --------------------------------------------------
    // DynamicTableSource
    // --------------------------------------------------

    @Override
    public DynamicTableSource copy() {
        return new MongoDynamicTableSource(tableSchema, mongoOptions, readOptions, lookupOptions);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    // --------------------------------------------------
    // ScanTableSource
    // --------------------------------------------------

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataDeserializationConverter deserConverter = new MongoRowDataDeserializationConverter(rowType);
        MongoRowDataSourceFunction<RowData> sourceFunction = new MongoRowDataSourceFunction<>(
                deserConverter,
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                readOptions.getFetchSize(),
                readOptions.isExcludeId()
        );
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    // --------------------------------------------------
    // LookupTableSource
    // --------------------------------------------------

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataDeserializationConverter deserConverter = new MongoRowDataDeserializationConverter(rowType);

        MongoRowDataLookupFunction lookupFunction = new MongoRowDataLookupFunction(
                deserConverter,
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                lookupOptions.getCacheMaxRows(),
                lookupOptions.getCacheTtl(),
                lookupOptions.getMaxRetries(),
                lookupOptions.isExcludeId()
        );
        return TableFunctionProvider.of(lookupFunction);
    }

    // --------------------------------------------------
    // SupportsProjectionPushDown
    // --------------------------------------------------

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {

    }

    // --------------------------------------------------------------
    // Function interfaces
    // --------------------------------------------------------------

    @FunctionalInterface
    private interface MongoGeoDeserializationConverter extends Serializable {
        Geometry deserialize(Object value); // Object: RowData
    }

    // --------------------------------------------------------------
    // Geospatial Deserialization converters (Geometry -> RowData)
    // --------------------------------------------------------------

    private MongoGeoDeserializationConverter createNullableExternalGeoConverter(LogicalType type) {
        return wrapIntoNullableExternalGeoConverter(createNotNullExternalGeoConverter(type), type);
    }

    private MongoGeoDeserializationConverter wrapIntoNullableExternalGeoConverter(
            MongoGeoDeserializationConverter converter, LogicalType type) {
        return value -> {
            if (value == null || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                throw new MongoTypeConversionException("Null is unsupported by GeoJSON");
            } else {
                return converter.deserialize(value);
            }
        };
    }

    private MongoGeoDeserializationConverter createNotNullExternalGeoConverter(LogicalType geoType) {
        if (isGeoMultiPointCoordinates(geoType)) {
            return value -> {
                RowData coordinates = (RowData) value;
                Position position = new Position(coordinates.getFloat(0), coordinates.getFloat(1));
                return new Point(position);
            };
        }
        if (isGeoLineStringCoordinates(geoType)) {
            return value -> {
                List<Position> positions = new ArrayList<>();

                RowData coordinates = (RowData) value;
                ArrayData arrayData = coordinates.getArray(0);
                for (int i = 0; i < arrayData.size(); i++) {
                    float[] positionData = arrayData.getArray(0).toFloatArray();
                    Position position = new Position(positionData[0], positionData[1]);
                    positions.add(position);
                }
                return new LineString(positions);
            };
        }
        if (isGeoPolygonCoordinates(geoType)) {
            return value -> {
                List<Position> positions = new ArrayList<>();

                return new Polygon(positions);
            };
        }
        if (isGeoMultiPointCoordinates(geoType)) {
            return value -> {
                List<Position> points = new ArrayList<>();
                RowData coordinates = (RowData) value;
                ArrayData arrayData = coordinates.getArray(0);
                for (int i = 0; i < arrayData.size(); i++) {
                    float[] positionData = arrayData.getArray(0).toFloatArray();
                    Position position = new Position(positionData[0], positionData[1]);
                    points.add(position);
                }
                return new MultiPoint(points);
            };
        }
        if (isGeoMultiLineStringCoordinates(geoType)) {
            return value -> {
                List<Position> positions = new ArrayList<>();

                RowData coordinates = (RowData) value;
                ArrayData lineArrayData = coordinates.getArray(0);

                return new MultiLineString(Collections.singletonList(positions));
            };
        }
        if (isGeoMultiPolygonCoordinates(geoType)) {
            return value -> {
                List<PolygonCoordinates> positions = new ArrayList<>();
                return new MultiPolygon(positions);
            };
        }
        throw new UnsupportedOperationException("Unsupported geo type:" + geoType);
    }


    // ~ utilities ------------------------------------------------------------------

    private boolean isGeoType(String[] fieldNames, LogicalType[] fieldTypes) {
        return //rowType.getFieldCount() == 2 &&
                GEO_TYPE_FIELD.equals(fieldNames[0])
                && fieldTypes[0].getTypeRoot().equals(LogicalTypeRoot.VARCHAR)
                && GEO_COORDINATES_FIELD.equals(fieldNames[1])
                && fieldTypes[1].getTypeRoot().equals(LogicalTypeRoot.ROW)
                && fieldTypes[1].getChildren().size() == 1
                && (isGeoLineStringCoordinates(fieldTypes[1]))
                ;
    }

    /**
     * <a href="https://tools.ietf.org/html/rfc7946#section-3.1.4">GeoJSON</a>
     */
    private boolean isSinglePosition(LogicalType coordinatesFieldType) {
        return coordinatesFieldType.getTypeRoot().equals(LogicalTypeRoot.ROW)
                && coordinatesFieldType.getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.FLOAT);
    }

    /**
     * <a href="https://tools.ietf.org/html/rfc7946#section-3.1.4">GeoJSON</a>
     */
    private boolean isMultiPosition(LogicalType coordinatesFieldType) {
        return coordinatesFieldType.getTypeRoot().equals(LogicalTypeRoot.ROW)
                && coordinatesFieldType.getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.ARRAY)
                && coordinatesFieldType.getChildren().get(0).getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.FLOAT);
    }

    /**
     * <a href="https://tools.ietf.org/html/rfc7946#section-3.1.4">GeoJSON</a>
     */
    private boolean isMultiLineString(LogicalType coordinatesFieldType) {
        return coordinatesFieldType.getTypeRoot().equals(LogicalTypeRoot.ROW)
                && coordinatesFieldType.getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.ARRAY)
                && coordinatesFieldType.getChildren().get(0).getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.ARRAY)
                && coordinatesFieldType.getChildren().get(0).getChildren().get(0).getChildren().size() == 1
                && coordinatesFieldType.getChildren().get(0).getChildren().get(0).getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.FLOAT);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<FLOAT>>
     */
    private boolean isGeoPointCoordinates(LogicalType coordinatesFieldType) {
        return isSinglePosition(coordinatesFieldType);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<ARRAY<FLOAT>>>
     */
    private boolean isGeoLineStringCoordinates(LogicalType coordinatesFieldType) {
        return isMultiPosition(coordinatesFieldType);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<ARRAY<ARRAY<FLOAT>>>>
     */
    private boolean isGeoPolygonCoordinates(LogicalType coordinatesFieldType) {
        return isMultiLineString(coordinatesFieldType);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<ARRAY<FLOAT>>>
     */
    private boolean isGeoMultiPointCoordinates(LogicalType coordinatesFieldType) {
        return isMultiPosition(coordinatesFieldType);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<ARRAY<ARRAY<FLOAT>>>>
     */
    private boolean isGeoMultiLineStringCoordinates(LogicalType coordinatesFieldType) {
        return isMultiLineString(coordinatesFieldType);
    }

    /**
     * ROW<`type` STRING, coordinates ARRAY<ARRAY<ARRAY<FLOAT>>>>
     */
    private boolean isGeoMultiPolygonCoordinates(LogicalType coordinatesFieldType) {
        return isMultiLineString(coordinatesFieldType);
    }
}
