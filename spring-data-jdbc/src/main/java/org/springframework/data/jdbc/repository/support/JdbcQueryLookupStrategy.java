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

import java.lang.reflect.Method;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.EntityRowMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.Lazy;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link QueryLookupStrategy} for JDBC repositories. Currently only supports annotated queries.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 */
public final class JdbcQueryLookupStrategy {

    private JdbcQueryLookupStrategy() {
    }

//    @RequiredArgsConstructor
    private abstract static class AbstractQueryLookupStrategy implements QueryLookupStrategy {

        protected final ApplicationEventPublisher publisher;
        protected final RelationalMappingContext context;
        protected final JdbcConverter converter;
        protected final DataAccessStrategy accessStrategy;
        protected final QueryMappingConfiguration queryMappingConfiguration;
        protected final NamedParameterJdbcOperations operations;


        /**
         * Creates a new {@link AbstractQueryLookupStrategy} for the given {@link RelationalMappingContext},
         * {@link DataAccessStrategy} and {@link QueryMappingConfiguration}.
         *
         * @param publisher                 must not be {@literal null}.
         * @param context                   must not be {@literal null}.
         * @param converter                 must not be {@literal null}.
         * @param accessStrategy            must not be {@literal null}.
         * @param queryMappingConfiguration must not be {@literal null}.
         */
        AbstractQueryLookupStrategy(ApplicationEventPublisher publisher, RelationalMappingContext context,
                                    JdbcConverter converter, DataAccessStrategy accessStrategy,
                                    QueryMappingConfiguration queryMappingConfiguration,
                                    NamedParameterJdbcOperations operations) {

            Assert.notNull(publisher, "Publisher must not be null!");
            Assert.notNull(context, "RelationalMappingContext must not be null!");
            Assert.notNull(converter, "RelationalConverter must not be null!");
            Assert.notNull(accessStrategy, "DataAccessStrategy must not be null!");
            Assert.notNull(queryMappingConfiguration, "RowMapperMap must not be null!");

            this.publisher = publisher;
            this.context = context;
            this.converter = converter;
            this.accessStrategy = accessStrategy;
            this.queryMappingConfiguration = queryMappingConfiguration;
            this.operations = operations;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework
         * .data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data
         * .repository.core.NamedQueries)
         */
        @Override
        public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
                                            ProjectionFactory projectionFactory, NamedQueries namedQueries) {
            JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, repositoryMetadata, projectionFactory);
            RowMapper<?> mapper = queryMethod.isModifyingQuery() ? null : createMapper(queryMethod);
            return resolveQuery(queryMethod, mapper);
        }

        protected abstract RepositoryQuery resolveQuery(JdbcQueryMethod jdbcQueryMethod, RowMapper<?> mapper);

        private RowMapper<?> createMapper(JdbcQueryMethod queryMethod) {

            Class<?> returnedObjectType = queryMethod.getReturnedObjectType();

            RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(returnedObjectType);

            if (persistentEntity == null) {
                return SingleColumnRowMapper.newInstance(returnedObjectType, converter.getConversionService());
            }

            return determineDefaultMapper(queryMethod);
        }

        private RowMapper<?> determineDefaultMapper(JdbcQueryMethod queryMethod) {

            Class<?> domainType = queryMethod.getReturnedObjectType();
            RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(domainType);

            if (configuredQueryMapper != null)
                return configuredQueryMapper;

            return new EntityRowMapper<>( //
                    context.getRequiredPersistentEntity(domainType), //
                    //
                    converter);
        }
    }

    private static class CreateQueryLookupStrategy extends AbstractQueryLookupStrategy {

        /**
         * {@link QueryLookupStrategy} to create a query from the method name.
         *
         * @param publisher                 must not be {@literal null}.
         * @param context                   must not be {@literal null}.
         * @param converter                 must not be {@literal null}.
         * @param accessStrategy            must not be {@literal null}.
         * @param queryMappingConfiguration must not be {@literal null}.
         * @param operations
         */
        public CreateQueryLookupStrategy(ApplicationEventPublisher publisher, RelationalMappingContext context, JdbcConverter converter,
                                         DataAccessStrategy accessStrategy, QueryMappingConfiguration queryMappingConfiguration,
                                         NamedParameterJdbcOperations operations) {
            super(publisher, context, converter, accessStrategy, queryMappingConfiguration, operations);
        }

        @Override
        protected RepositoryQuery resolveQuery(JdbcQueryMethod jdbcQueryMethod, RowMapper<?> mapper) {
            return new PartTreeJdbcRepositoryQuery(publisher, context, jdbcQueryMethod, operations, mapper);
        }
    }

    private static class DeclaredQueryLookupStrategy extends AbstractQueryLookupStrategy {

        /**
         * Creates a new {@link AbstractQueryLookupStrategy} for the given {@link RelationalMappingContext},
         * {@link DataAccessStrategy} and {@link QueryMappingConfiguration}.
         *
         * @param publisher                 must not be {@literal null}.
         * @param context                   must not be {@literal null}.
         * @param converter                 must not be {@literal null}.
         * @param accessStrategy            must not be {@literal null}.
         * @param queryMappingConfiguration must not be {@literal null}.
         * @param operations
         */
        public DeclaredQueryLookupStrategy(ApplicationEventPublisher publisher, RelationalMappingContext context, JdbcConverter converter,
                                           DataAccessStrategy accessStrategy, QueryMappingConfiguration queryMappingConfiguration,
                                           NamedParameterJdbcOperations operations) {
            super(publisher, context, converter, accessStrategy, queryMappingConfiguration, operations);
        }

        @Override
        protected RepositoryQuery resolveQuery(JdbcQueryMethod jdbcQueryMethod, RowMapper<?> mapper) {
            return new DeclaredJdbcRepositoryQuery(publisher, context, jdbcQueryMethod, operations, mapper);
        }
    }


    /**
     * {@link QueryLookupStrategy} to try to detect a declared query first (
     * {@link org.springframework.data.jdbc.repository.query.Query}, named query). In case none is found we fall back on
     * query creation.
     */
    private static class CreateIfNotFoundQueryLookupStrategy extends AbstractQueryLookupStrategy {

        private final CreateQueryLookupStrategy createStrategy;
        private final DeclaredQueryLookupStrategy lookupStrategy;

        /**
         * Creates a new {@link CreateIfNotFoundQueryLookupStrategy} for the given {@link RelationalMappingContext},
         * {@link DataAccessStrategy} and {@link QueryMappingConfiguration}.
         *
         * @param publisher                 must not be {@literal null}.
         * @param context                   must not be {@literal null}.
         * @param converter                 must not be {@literal null}.
         * @param accessStrategy            must not be {@literal null}.
         * @param queryMappingConfiguration must not be {@literal null}.
         * @param operations
         * @param lookupStrategy
         * @param createStrategy
         */
        public CreateIfNotFoundQueryLookupStrategy(ApplicationEventPublisher publisher, RelationalMappingContext context,
                                                   JdbcConverter converter, DataAccessStrategy accessStrategy,
                                                   QueryMappingConfiguration queryMappingConfiguration,
                                                   NamedParameterJdbcOperations operations,
                                                   CreateQueryLookupStrategy createStrategy, DeclaredQueryLookupStrategy lookupStrategy) {
            super(publisher, context, converter, accessStrategy, queryMappingConfiguration, operations);
            this.createStrategy = createStrategy;
            this.lookupStrategy = lookupStrategy;
        }

        @Override
        protected RepositoryQuery resolveQuery(JdbcQueryMethod jdbcQueryMethod, RowMapper<?> mapper) {
            if (jdbcQueryMethod.getAnnotatedQuery() != null) {
                return lookupStrategy.resolveQuery(jdbcQueryMethod, mapper);
            } else {
                return createStrategy.resolveQuery(jdbcQueryMethod, mapper);
            }
        }
    }

    public static QueryLookupStrategy create(@Nullable QueryLookupStrategy.Key key, ApplicationEventPublisher publisher,
                                              RelationalMappingContext context, JdbcConverter converter, DataAccessStrategy accessStrategy,
                                              QueryMappingConfiguration queryMappingConfiguration,
                                              NamedParameterJdbcOperations operations) {

        Lazy<CreateQueryLookupStrategy> createQueryLookupStrategyLazy = Lazy.of(() ->
                new CreateQueryLookupStrategy(publisher, context, converter, accessStrategy, queryMappingConfiguration, operations));

        Lazy<DeclaredQueryLookupStrategy> declaredQueryLookupStrategyLazy = Lazy.of(() ->
                new DeclaredQueryLookupStrategy(publisher, context, converter, accessStrategy, queryMappingConfiguration, operations));

        switch (key != null ? key : QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND) {
            case CREATE:
                return createQueryLookupStrategyLazy.get();
            case USE_DECLARED_QUERY:
                return declaredQueryLookupStrategyLazy.get();
            case CREATE_IF_NOT_FOUND:
                return new CreateIfNotFoundQueryLookupStrategy(publisher, context, converter, accessStrategy, queryMappingConfiguration,
                        operations, createQueryLookupStrategyLazy.get(), declaredQueryLookupStrategyLazy.get());
            default:
                throw new IllegalArgumentException(String.format("Unsupported query lookup strategy %s!", key));
        }
    }
}