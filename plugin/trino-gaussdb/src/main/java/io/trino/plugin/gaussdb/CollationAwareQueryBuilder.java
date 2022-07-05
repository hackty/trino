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
package io.trino.plugin.gaussdb;

import io.trino.plugin.jdbc.*;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.plugin.gaussdb.GaussDbClient.isCollatable;
import static java.lang.String.format;


public class CollationAwareQueryBuilder
        extends DefaultQueryBuilder
{
    @Override
    protected String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        boolean isCollatable = Stream.of(condition.getLeftColumn(), condition.getRightColumn())
                .anyMatch(GaussDbClient::isCollatable);

        if (isCollatable) {
            return format(
                    "%s.%s COLLATE \"C\" %s %s.%s COLLATE \"C\"",
                    leftRelationAlias,
                    buildJoinColumn(client, condition.getLeftColumn()),
                    condition.getOperator().getValue(),
                    rightRelationAlias,
                    buildJoinColumn(client, condition.getRightColumn()));
        }

        return super.formatJoinCondition(client, leftRelationAlias, rightRelationAlias, condition);
    }

    @Override
    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        if (isCollatable(column)) {
            accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
            return format("%s %s %s COLLATE \"C\"", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
        }

        return super.toPredicate(client, session, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }
}
