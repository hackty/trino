/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.aggregation.*;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.*;

import javax.inject.Inject;
import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.*;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;


public class TeradataClient extends BaseJdbcClient {


    private static final int TD_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;

    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;

    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;

    @Inject
    public TeradataClient(BaseJdbcConfig config,
                          JdbcStatisticsConfig statisticsConfig,
                          ConnectionFactory connectionFactory,
                          QueryBuilder queryBuilder,
                          TypeManager typeManager,
                          IdentifierMapping identifierMapping) {
        super(config, "", connectionFactory, queryBuilder, identifierMapping);
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL)))
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like_pattern(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like_pattern(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementMinMax(false))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames) {
        String sql = format(
                "CREATE TABLE %s AS ( SELECT %s FROM %s ) WITH NO DATA",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    //    @Override
//    public Collection<String> listSchemas(Connection connection) {
//        try(ResultSet resultSet = connection.getMetaData().getSchemas()){
//            ImmutableSet.Builder<String> schemas = ImmutableSet.builder();
//            while (resultSet.next()){
//                String schema = resultSet.getString(1).toLowerCase(Locale.ROOT);
//                schemas.add(schema);
//            }
//            return schemas.build();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    @Override
//    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName) throws SQLException {
//        DatabaseMetaData metadata = connection.getMetaData();
//        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
//        return metadata.getTables(
//                null,
//                escapeNamePattern(schemaName, escape).orElse(null),
//                escapeNamePattern(tableName, escape).orElse(null),
//                new String[] {"TABLE", "VIEW", "SYNONYM"});
//    }

//    @Nullable
//    @Override
//    public JdbcTableHandle getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName) {
//        try (Connection connection = connectionFactory.openConnection(identity)) {
//            DatabaseMetaData databaseMetaData = connection.getMetaData();
//            String jdbcSchemaName = schemaTableName.getSchemaName();
//            String jdbcTableName = schemaTableName.getTableName();
//            if(databaseMetaData.storesUpperCaseIdentifiers()){
//                jdbcSchemaName = jdbcSchemaName.toUpperCase(Locale.ROOT);
//                jdbcTableName = jdbcTableName.toUpperCase(Locale.ROOT);
//            }
//            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
//                List<JdbcTableHandle> tableHandles = new ArrayList<>();
//                while (resultSet.next()) {
//                    tableHandles.add(new JdbcTableHandle(
//                            connectorId,
//                            schemaTableName,
//                            resultSet.getString("TABLE_CAT"),
//                            resultSet.getString("TABLE_SCHEM"),
//                            resultSet.getString("TABLE_NAME")));
//                }
//                if (tableHandles.isEmpty()) {
//                    return null;
//                }
//                if (tableHandles.size() > 1) {
//                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
//                }
//                return getOnlyElement(tableHandles);
//            }
//        }
//        catch (SQLException e) {
//            throw new PrestoException(JDBC_ERROR, e);
//        }
//    }


    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

//        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
//            case "tinyint unsigned":
//                return Optional.of(smallintColumnMapping());
//            case "smallint unsigned":
//                return Optional.of(integerColumnMapping());
//            case "int unsigned":
//                return Optional.of(bigintColumnMapping());
//            case "bigint unsigned":
//                return Optional.of(decimalColumnMapping(createDecimalType(20)));
//            case "json":
//                return Optional.of(jsonColumnMapping());
//        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does mysql support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.getRequiredColumnSize()));
                return Optional.of(timeColumnMapping(timeType));

            case Types.TIMESTAMP:
                int precisions = typeHandle.getRequiredDecimalDigits();
                return Optional.of(timestampColumnMapping(createTimestampType(precisions)));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private static int getTimePrecision(int timeColumnSize)
    {
//        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
//            return 0;
//        }
//        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        return 0;
    }

//    private static int getTimestampPrecision(int timestampColumnSize)
//    {
//        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
//            return 0;
//        }
//        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
//        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
//        return timestampPrecision;
//    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            // PostgreSQL has no type corresponding to tinyint
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType) {
            TimeType timeType = (TimeType) type;
            if (timeType.getPrecision() <= TD_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", TD_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWriteFunction(TD_MAX_SUPPORTED_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            int precision = min(timestampType.getPrecision(), TD_MAX_SUPPORTED_TIMESTAMP_PRECISION);
            String dataType = format("datetime2(%d)", precision);
            if (timestampType.getPrecision() <= MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(timestampType, precision));
        }
//        if (type instanceof TimestampWithTimeZoneType) {
//            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
//            if (timestampWithTimeZoneType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
//                String dataType = format("timestamptz(%d)", timestampWithTimeZoneType.getPrecision());
//                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
//                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
//                }
//                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
//            }
//            return WriteMapping.objectMapping(format("timestamptz(%d)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
//        }
//        if (type.equals(jsonType)) {
//            return WriteMapping.sliceMapping("jsonb", typedVarcharWriteFunction("json"));
//        }
//        if (type.equals(uuidType)) {
//            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
//        }
//        if (type instanceof ArrayType && getArrayMapping(session) == AS_ARRAY) {
//            Type elementType = ((ArrayType) type).getElementType();
//            String elementDataType = toWriteMapping(session, elementType).getDataType();
//            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
//        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String remoteSchemaName, String remoteTableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            ConnectorIdentity identity = session.getIdentity();
            String newRemoteSchemaName = getIdentifierMapping().toRemoteSchemaName(identity, connection, newSchemaName);
            String newRemoteTableName = getIdentifierMapping().toRemoteTableName(identity, connection, newRemoteSchemaName, newTableName);
            String sql = format(
                    "RENAME TABLE %s AS %s",
                    quoted(catalogName, remoteSchemaName, remoteTableName),
                    quoted(catalogName, newRemoteSchemaName, newRemoteTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Postgres sorts textual types differently compared to Trino so we cannot safely pushdown any aggregations which take a text type as an input or as part of grouping set
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<String> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static final PredicatePushdownController POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN = (session, domain) -> FULL_PUSHDOWN.apply(session, domain);

}
