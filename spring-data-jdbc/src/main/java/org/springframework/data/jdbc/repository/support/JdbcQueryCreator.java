package org.springframework.data.jdbc.repository.support;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.SqlGenerator;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Iterator;

import static org.springframework.data.repository.query.parser.Part.Type;


public class JdbcQueryCreator extends AbstractQueryCreator<String, Condition> {

    private final SqlGeneratorSource sqlGeneratorSource;
    private final ReturnedType type;
    private final Class<?> root;
    private final SqlGenerator sqlGenerator;

    // TODO: maybe i can pass in mapping context and use "mappingContext.getPersistentPropertyPath(property)"
    public JdbcQueryCreator(PartTree tree, SqlGeneratorSource sqlGeneratorSource, ReturnedType type) {
        super(tree);
        this.sqlGeneratorSource = sqlGeneratorSource;
        this.type = type;
        this.root = type.getDomainType();
        this.sqlGenerator = sqlGeneratorSource.getSqlGenerator(this.root);
    }

    @Override
    protected Condition create(Part part, Iterator<Object> iterator) {
        return toCondition(part);
    }

    @Override
    protected Condition and(Part part, Condition condition, Iterator<Object> iterator) {
        return condition.and(toCondition(part));
    }

    @Override
    protected Condition or(Condition condition1, Condition condition2) {
        return condition1.or(condition2);
    }

    @Override
    protected String complete(Condition condition, Sort sort) {
        return sqlGenerator.render(sqlGenerator.selectBuilder().where(condition).build());
    }

    private Condition toCondition(final Part part) {

        final PropertyPath property = part.getProperty();
        final Type type = part.getType();


        switch (type) {

            case BETWEEN:
//                Conditions.
            case AFTER:
            case GREATER_THAN:
//                return Conditions.isGreater(new Expressions.SimpleExpression())
            case GREATER_THAN_EQUAL:
            case BEFORE:
            case LESS_THAN:
            case LESS_THAN_EQUAL:
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT_IN:
            case IN:
            case STARTING_WITH:
            case ENDING_WITH:
            case CONTAINING:
            case NOT_CONTAINING:
            case LIKE:
            case NOT_LIKE:
            case TRUE:
            case FALSE:
            case SIMPLE_PROPERTY:
                return Conditions.isEqual(sqlGenerator.getColumn(property), SQL.bindMarker());
            case NEGATING_SIMPLE_PROPERTY:
            case IS_EMPTY:
            case IS_NOT_EMPTY:


//            case NEAR:
//                break;
//            case WITHIN:
//                break;
//            case REGEX:
//                break;
//            case EXISTS:
//                break;
//            case TRUE:
//                break;


            default:
                throw new IllegalArgumentException("Unsupported keyword " + type);
        }
    }
}
