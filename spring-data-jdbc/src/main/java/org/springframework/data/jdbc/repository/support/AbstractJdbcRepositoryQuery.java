/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.support;

import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;

/**
 * A query to be executed based on a repository method, it's annotated SQL query and the arguments provided to the
 * method.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 */
abstract class AbstractJdbcRepositoryQuery implements RepositoryQuery {

    protected final ApplicationEventPublisher publisher;
    protected final RelationalMappingContext context;
    protected final JdbcQueryMethod queryMethod;
    protected final NamedParameterJdbcOperations operations;
    protected final JdbcQueryExecutor executor;

    /**
     * Creates a new {@link AbstractJdbcRepositoryQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
     * and {@link RowMapper}.
     *
     * @param publisher        must not be {@literal null}.
     * @param context          must not be {@literal null}.
     * @param queryMethod      must not be {@literal null}.
     * @param operations       must not be {@literal null}.
     * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
     */
    AbstractJdbcRepositoryQuery(ApplicationEventPublisher publisher, RelationalMappingContext context,
                                JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations, RowMapper<?> defaultRowMapper) {

        Assert.notNull(publisher, "Publisher must not be null!");
        Assert.notNull(context, "Context must not be null!");
        Assert.notNull(queryMethod, "Query method must not be null!");
        Assert.notNull(operations, "NamedParameterJdbcOperations must not be null!");

        if (!queryMethod.isModifyingQuery()) {
            Assert.notNull(defaultRowMapper, "Mapper must not be null!");
        }

        this.publisher = publisher;
        this.context = context;
        this.queryMethod = queryMethod;
        this.operations = operations;

        RowMapper rowMapper = determineRowMapper(defaultRowMapper);
        ResultSetExtractor extractor = determineResultSetExtractor(rowMapper != defaultRowMapper ? rowMapper : null);

        executor = JdbcQueryExecutor.create(queryMethod, getQuery(), operations, useNamedParameters(), isModifyingQuery(), extractor,
                rowMapper, this::publishAfterLoad);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
     */
    @Override
    public Object execute(Object[] objects) {
        return executor.doExecute(objects);
    }

    protected abstract String getQuery();

    protected abstract boolean isModifyingQuery();

    protected abstract boolean useNamedParameters();

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
     */
    @Override
    public JdbcQueryMethod getQueryMethod() {
        return queryMethod;
    }

    @Nullable
    private ResultSetExtractor determineResultSetExtractor(@Nullable RowMapper<?> rowMapper) {

        Class<? extends ResultSetExtractor> resultSetExtractorClass = queryMethod.getResultSetExtractorClass();

        if (isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class)) {
            return null;
        }

        Constructor<? extends ResultSetExtractor> constructor = ClassUtils
                .getConstructorIfAvailable(resultSetExtractorClass, RowMapper.class);

        if (constructor != null) {
            return BeanUtils.instantiateClass(constructor, rowMapper);
        }

        return BeanUtils.instantiateClass(resultSetExtractorClass);
    }

    private RowMapper determineRowMapper(RowMapper<?> defaultMapper) {

        Class<?> rowMapperClass = queryMethod.getRowMapperClass();

        if (isUnconfigured(rowMapperClass, RowMapper.class)) {
            return defaultMapper;
        }

        return (RowMapper) BeanUtils.instantiateClass(rowMapperClass);
    }

    private static boolean isUnconfigured(@Nullable Class<?> configuredClass, Class<?> defaultClass) {
        return configuredClass == null || configuredClass == defaultClass;
    }

    private <T> void publishAfterLoad(@Nullable T entity) {

        if (entity != null && context.hasPersistentEntityFor(entity.getClass())) {

            RelationalPersistentEntity<?> e = context.getRequiredPersistentEntity(entity.getClass());
            Object identifier = e.getIdentifierAccessor(entity).getIdentifier();

            if (identifier != null) {
                publisher.publishEvent(new AfterLoadEvent(Identifier.of(identifier), entity));
            }
        }
    }

}
