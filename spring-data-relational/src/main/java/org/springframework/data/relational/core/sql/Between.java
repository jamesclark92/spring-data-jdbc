package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

public class Between extends AbstractSegment implements Condition {

    private final Expression left;
    private final Expression middle;
    private final Expression right;
    private final boolean negated;

    private Between(Expression left, Expression middle, Expression right) {
        this(left, middle, right, false);
    }

    private Between(Expression left, Expression middle, Expression right, boolean negated) {
        super(left, middle, right);
        this.left = left;
        this.middle = middle;
        this.right = right;
        this.negated = negated;
    }

    /**
     * Creates a new {@link Between} {@link Condition} given left, middle, and right {@link Expression}s.
     *
     * @param comparisonColumnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
     * @param leftExpression               left hand side of the {@link Condition} must not be {@literal null}.
     * @param rightExpression              left hand side of the {@link Condition} must not be {@literal null}.
     * @return the {@link Between} {@link Condition}.
     */
    public static Between create(Expression comparisonColumnOrExpression, Expression leftExpression, Expression rightExpression) {

        Assert.notNull(comparisonColumnOrExpression, "Comparison column or expression must not be null");
        Assert.notNull(leftExpression, "Left expression must not be null");
        Assert.notNull(rightExpression, "Right expression must not be null");

        return new Between(comparisonColumnOrExpression, leftExpression, rightExpression);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.core.sql.Condition#not()
     */
    @Override
    public Condition not() {
        return new Between(left, middle, right, !negated);
    }

    /**
     * @return the left {@link Expression}.
     */
    public Expression getLeft() {
        return left;
    }

    /**
     * @return the middle {@link Expression}.
     */
    public Expression getMiddle() {
        return middle;
    }

    /**
     * @return the right {@link Expression}.
     */
    public Expression getRight() {
        return right;
    }

    /**
     * @return if this condition is negated or not.
     */
    public boolean isNegated() {
        return negated;
    }

    @Override
    public String toString() {
        return left + (negated ? " NOT BETWEEN " : " BETWEEN ") + middle.toString() + " AND " + right.toString();
    }
}
