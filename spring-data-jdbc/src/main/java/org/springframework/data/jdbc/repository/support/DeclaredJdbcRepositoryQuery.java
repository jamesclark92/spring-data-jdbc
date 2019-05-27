package org.springframework.data.jdbc.repository.support;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.StringUtils;

class DeclaredJdbcRepositoryQuery extends AbstractJdbcRepositoryQuery {


    /**
     * Creates a new {@link DeclaredJdbcRepositoryQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
     * and {@link RowMapper}. The given {@link JdbcQueryMethod} must be annotated with
     * {@link org.springframework.data.jdbc.repository.query.Query Query}.
     *
     * @param publisher        must not be {@literal null}.
     * @param context          must not be {@literal null}.
     * @param queryMethod      must not be {@literal null}.
     * @param operations       must not be {@literal null}.
     * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
     */
    DeclaredJdbcRepositoryQuery(ApplicationEventPublisher publisher, RelationalMappingContext context, JdbcQueryMethod queryMethod,
                                NamedParameterJdbcOperations operations, RowMapper<?> defaultRowMapper) {
        super(publisher, context, queryMethod, operations, defaultRowMapper);
    }

    @Override
    protected String getQuery() {
        String query = queryMethod.getAnnotatedQuery();

        if (StringUtils.isEmpty(query)) {
            throw new IllegalStateException(String.format("No query specified on %s", queryMethod.getName()));
        }

        return query;
    }

    @Override
    protected boolean isModifyingQuery() {
        return this.queryMethod.isModifyingQuery();
    }

    @Override
    protected boolean useNamedParameters() {
        return true;
    }

}
