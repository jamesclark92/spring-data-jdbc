package org.springframework.data.jdbc.repository.support;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.List;
import java.util.function.Consumer;

public final class JdbcQueryExecutor<U> {

    private final JdbcQueryMethod jdbcQueryMethod;
    private final Consumer<Object> publishAfterLoad;
    private final Operations<U> operations;
    private final QueryExecutor<Object, U> queryExecutor;

    private JdbcQueryExecutor(JdbcQueryMethod jdbcQueryMethod, String query, Operations<U> operations, boolean isModifyingQuery,
                              @Nullable ResultSetExtractor extractor, RowMapper rowMapper, Consumer<Object> publishAfterLoad) {
        this.jdbcQueryMethod = jdbcQueryMethod;
        this.operations = operations;
        this.publishAfterLoad = publishAfterLoad;
        this.queryExecutor = createExecutor(jdbcQueryMethod, query, isModifyingQuery, extractor, rowMapper);
    }

    @Nullable
    public Object doExecute(Object[] objects) {
        return queryExecutor.execute(operations.bindParameters(objects, jdbcQueryMethod));
    }

    private QueryExecutor<Object, U> createExecutor(JdbcQueryMethod queryMethod, String query, boolean isModifyingQuery,
                                                    @Nullable ResultSetExtractor extractor, RowMapper rowMapper) {

        if (isModifyingQuery) {
            return new ModifyingQueryExecutor<>(query, operations, jdbcQueryMethod);
        }

        if (queryMethod.isCollectionQuery() || queryMethod.isStreamQuery()) {
            QueryExecutor<Object, U> innerExecutor = extractor != null ?
                    new ResultSetExtractorQueryExecutor<>(query, operations, extractor) :
                    new ListRowMapperQueryExecutor<>(query, operations, rowMapper);
            return new CollectionQueryExecutor<>(innerExecutor, entities -> entities.forEach(publishAfterLoad));
        }

        QueryExecutor<Object, U> innerExecutor = extractor != null ?
                new ResultSetExtractorQueryExecutor<>(query, operations, extractor) :
                new ObjectRowMapperQueryExecutor<>(query, operations, rowMapper);
        return new ObjectQueryExecutor<>(innerExecutor, publishAfterLoad);
    }


    private static class ObjectQueryExecutor<T, U> implements QueryExecutor<T, U> {

        private final QueryExecutor<T, U> innerExecutor;
        private final Consumer<T> publishAfterLoad;

        private ObjectQueryExecutor(QueryExecutor<T, U> innerExecutor, Consumer<T> publishAfterLoad) {
            this.innerExecutor = innerExecutor;
            this.publishAfterLoad = publishAfterLoad;
        }

        @Override
        public T execute(U parameters) {
            try {
                T result = innerExecutor.execute(parameters);
                publishAfterLoad.accept(result);
                return result;
            } catch (EmptyResultDataAccessException e) {
                return null;
            }
        }
    }

    private static class CollectionQueryExecutor<T, U> implements QueryExecutor<T, U> {

        private final QueryExecutor<T, U> innerExecutor;
        private final Consumer<List<?>> publishAfterLoad;

        private CollectionQueryExecutor(QueryExecutor<T, U> innerExecutor, Consumer<List<?>> publishAfterLoad) {
            this.innerExecutor = innerExecutor;
            this.publishAfterLoad = publishAfterLoad;
        }

        @Override
        public T execute(U parameters) {
            T result = innerExecutor.execute(parameters);
            Assert.notNull(result, "A collection valued result must never be null.");
            publishAfterLoad.accept((List<?>) result);
            return result;
        }
    }

    private static abstract class AbstractInnerQueryExecutor<T, U> implements QueryExecutor<T, U> {
        protected final String query;
        protected final Operations<U> operations;

        private AbstractInnerQueryExecutor(String query, Operations<U> operations) {
            this.query = query;
            this.operations = operations;
        }
    }

    private static class ModifyingQueryExecutor<U> extends AbstractInnerQueryExecutor<Object, U> {

        private final JdbcQueryMethod jdbcQueryMethod;

        private ModifyingQueryExecutor(String query, Operations<U> operations, JdbcQueryMethod jdbcQueryMethod) {
            super(query, operations);
            this.jdbcQueryMethod = jdbcQueryMethod;
        }

        @Override
        public Object execute(U parameters) {
            int updatedCount = operations.update(query, parameters);
            Class<?> returnedObjectType = jdbcQueryMethod.getReturnedObjectType();

            return (returnedObjectType == boolean.class || returnedObjectType == Boolean.class) ? updatedCount != 0
                    : updatedCount;
        }
    }

    private static class ListRowMapperQueryExecutor<U> extends AbstractInnerQueryExecutor<Object, U> {

        private final RowMapper<?> rowMapper;

        private ListRowMapperQueryExecutor(String query, Operations<U> operations, RowMapper<?> rowMapper) {
            super(query, operations);
            this.rowMapper = rowMapper;
        }

        @Override
        public Object execute(U parameters) {
            return operations.queryForListRowMapper(query, parameters, rowMapper);
        }
    }

    private static class ObjectRowMapperQueryExecutor<U> extends AbstractInnerQueryExecutor<Object, U> {

        private final RowMapper<?> rowMapper;

        private ObjectRowMapperQueryExecutor(String query, Operations<U> operations, RowMapper<?> rowMapper) {
            super(query, operations);
            this.rowMapper = rowMapper;
        }

        @Override
        public Object execute(U parameters) {
            return operations.queryForObjectRowMapper(query, parameters, rowMapper);
        }
    }

    private static class ResultSetExtractorQueryExecutor<U> extends AbstractInnerQueryExecutor<Object, U> {

        private final ResultSetExtractor<?> resultSetExtractor;

        private ResultSetExtractorQueryExecutor(String query, Operations<U> operations, ResultSetExtractor<?> resultSetExtractor) {
            super(query, operations);
            this.resultSetExtractor = resultSetExtractor;
        }

        @Override
        public Object execute(U parameters) {
            return operations.queryWithResultSetExtractor(query, parameters, resultSetExtractor);
        }
    }

    private interface QueryExecutor<T, U> {
        @Nullable
        T execute(U parameters);
    }

    private interface Operations<T> {

        T bindParameters(Object[] objects, JdbcQueryMethod jdbcQueryMethod);

        int update(String sql, T parameters);

        <R> List<R> queryForListRowMapper(String sql, T parameters, RowMapper<R> rowMapper);

        @Nullable
        <R> R queryForObjectRowMapper(String sql, T parameters, RowMapper<R> rowMapper);

        @Nullable
        <R> R queryWithResultSetExtractor(String sql, T parameters, ResultSetExtractor<R> resultSetExtractor);
    }

    private static class NonNamedParameterOperations implements Operations<Object[]> {

        private final JdbcOperations jdbcOperations;

        private NonNamedParameterOperations(JdbcOperations jdbcOperations) {
            this.jdbcOperations = jdbcOperations;
        }

        @Override
        public Object[] bindParameters(Object[] objects, JdbcQueryMethod jdbcQueryMethod) {
            return objects;
        }

        @Override
        public int update(String sql, Object[] parameters) {
            return jdbcOperations.update(sql, parameters);
        }

        @Override
        public <R> List<R> queryForListRowMapper(String sql, Object[] parameters, RowMapper<R> rowMapper) {
            return jdbcOperations.query(sql, parameters, rowMapper);
        }

        @Override
        public <R> R queryForObjectRowMapper(String sql, Object[] parameters, RowMapper<R> rowMapper) {
            return jdbcOperations.queryForObject(sql, parameters, rowMapper);
        }

        @Override
        public <R> R queryWithResultSetExtractor(String sql, Object[] parameters, ResultSetExtractor<R> resultSetExtractor) {
            return jdbcOperations.query(sql, parameters, resultSetExtractor);
        }
    }

    private static class NamedParameterOperations implements Operations<MapSqlParameterSource> {

        private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method" +
                " parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

        private final NamedParameterJdbcOperations namedParameterJdbcOperations;

        private NamedParameterOperations(NamedParameterJdbcOperations namedParameterJdbcOperations) {
            this.namedParameterJdbcOperations = namedParameterJdbcOperations;
        }

        @Override
        public MapSqlParameterSource bindParameters(Object[] objects, JdbcQueryMethod jdbcQueryMethod) {
            MapSqlParameterSource parameters = new MapSqlParameterSource();

            jdbcQueryMethod.getParameters().getBindableParameters().forEach(p -> {

                String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
                parameters.addValue(parameterName, objects[p.getIndex()]);
            });

            return parameters;
        }

        @Override
        public int update(String sql, MapSqlParameterSource parameters) {
            return namedParameterJdbcOperations.update(sql, parameters);
        }

        @Override
        public <R> List<R> queryForListRowMapper(String sql, MapSqlParameterSource parameters, RowMapper<R> rowMapper) {
            return namedParameterJdbcOperations.query(sql, parameters, rowMapper);
        }

        @Override
        public <R> R queryForObjectRowMapper(String sql, MapSqlParameterSource parameters, RowMapper<R> rowMapper) {
            return namedParameterJdbcOperations.queryForObject(sql, parameters, rowMapper);
        }

        @Override
        public <R> R queryWithResultSetExtractor(String sql, MapSqlParameterSource parameters, ResultSetExtractor<R> resultSetExtractor) {
            return namedParameterJdbcOperations.query(sql, parameters, resultSetExtractor);
        }
    }

    public static JdbcQueryExecutor create(JdbcQueryMethod jdbcQueryMethod, String query,
                                           NamedParameterJdbcOperations namedParameterJdbcOperations, boolean useNamedParameters,
                                           boolean isModifyingQuery, @Nullable ResultSetExtractor extractor, RowMapper rowMapper,
                                           Consumer<Object> publishAfterLoad) {
        if (useNamedParameters) {
            return new JdbcQueryExecutor<>(jdbcQueryMethod, query, new NamedParameterOperations(namedParameterJdbcOperations),
                    isModifyingQuery, extractor, rowMapper, publishAfterLoad);
        } else {
            return new JdbcQueryExecutor<>(jdbcQueryMethod, query,
                    new NonNamedParameterOperations(namedParameterJdbcOperations.getJdbcOperations()), isModifyingQuery, extractor,
                    rowMapper, publishAfterLoad);
        }
    }
}
