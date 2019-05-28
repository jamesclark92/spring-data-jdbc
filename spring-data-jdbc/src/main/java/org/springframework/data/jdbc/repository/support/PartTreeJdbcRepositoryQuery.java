package org.springframework.data.jdbc.repository.support;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

class PartTreeJdbcRepositoryQuery extends AbstractJdbcRepositoryQuery {

    private final PartTree partTree;
    private final Parameters<?, ?> parameters;
    private final JdbcQueryCreator jdbcQueryCreator;
    private SqlGeneratorSource sqlGeneratorSource;
    private final String cachedQuery;

    /**
     * Creates a new {@link PartTreeJdbcRepositoryQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
     * and {@link RowMapper}.
     *  @param publisher        must not be {@literal null}.
     * @param context          must not be {@literal null}.
     * @param queryMethod      must not be {@literal null}.
     * @param operations       must not be {@literal null}.
     * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
     * @param sqlGeneratorSource
     */
    PartTreeJdbcRepositoryQuery(ApplicationEventPublisher publisher, RelationalMappingContext context, JdbcQueryMethod queryMethod,
                                NamedParameterJdbcOperations operations, RowMapper<?> defaultRowMapper,
                                SqlGeneratorSource sqlGeneratorSource) {
        super(publisher, context, queryMethod, operations, defaultRowMapper);

        this.partTree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
        this.parameters = queryMethod.getParameters();
        this.sqlGeneratorSource = sqlGeneratorSource;
        this.jdbcQueryCreator = new JdbcQueryCreator(partTree, sqlGeneratorSource, queryMethod.getResultProcessor().getReturnedType());

        if (!partTree.isEmpty()) {
            Assert.notNull(defaultRowMapper, "Mapper must not be null!");
        }
        boolean recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically();

        this.cachedQuery = jdbcQueryCreator.createQuery();
    }

    @Override
    protected String getQuery() {
        return cachedQuery;
    }

    @Override
    protected boolean isModifyingQuery() {
        return partTree.isDelete();
    }

    @Override
    protected boolean useNamedParameters() {
        return false;
    }

}
