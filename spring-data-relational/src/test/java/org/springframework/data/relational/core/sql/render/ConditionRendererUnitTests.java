/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for rendered {@link org.springframework.data.relational.core.sql.Conditions}.
 *
 * @author Mark Paluch
 */
public class ConditionRendererUnitTests {

	Table table = Table.create("my_table");
	Column left = table.column("left");
	Column right = table.column("right");

	@Test // DATAJDBC-309
	public void shouldRenderEquals() {

		String sql = SqlRenderer
				.toString(StatementBuilder.select(left).from(table).where(left.isEqualTo(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left = my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderNotEquals() {

		String sql = SqlRenderer
				.toString(StatementBuilder.select(left).from(table).where(left.isNotEqualTo(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left != my_table.right");

		sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.isEqualTo(right).not()).build());

		assertThat(sql).endsWith("WHERE my_table.left != my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsLess() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.isLess(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left < my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsLessOrEqualTo() {

		String sql = SqlRenderer
				.toString(StatementBuilder.select(left).from(table).where(left.isLessOrEqualTo(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left <= my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsGreater() {

		String sql = SqlRenderer
				.toString(StatementBuilder.select(left).from(table).where(left.isGreater(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left > my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsGreaterOrEqualTo() {

		String sql = SqlRenderer
				.toString(StatementBuilder.select(left).from(table).where(left.isGreaterOrEqualTo(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left >= my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIn() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.in(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left IN (my_table.right)");
	}

    @Test // DATAJDBC-309
    public void shouldRenderNotIn() {

        String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.notIn(right)).build());

        assertThat(sql).endsWith("WHERE my_table.left NOT IN (my_table.right)");
    }

	@Test // DATAJDBC-309
	public void shouldRenderLike() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.like(right)).build());

		assertThat(sql).endsWith("WHERE my_table.left LIKE my_table.right");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsNull() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.isNull()).build());

		assertThat(sql).endsWith("WHERE my_table.left IS NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsNotNull() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.isNotNull()).build());

		assertThat(sql).endsWith("WHERE my_table.left IS NOT NULL");

		sql = SqlRenderer.toString(StatementBuilder.select(left).from(table).where(left.isNull().not()).build());

		assertThat(sql).endsWith("WHERE my_table.left IS NOT NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderBetween() {

		String sql = SqlRenderer.toString(StatementBuilder.select(left)
														  .from(table)
														  .where(left.between(SQL.literalOf(1), SQL.literalOf(2)))
														  .build());

		assertThat(sql).endsWith("WHERE my_table.left BETWEEN 1 AND 2");
	}

    @Test // DATAJDBC-309
    public void shouldRenderNotBetween() {

        String sql = SqlRenderer.toString(StatementBuilder.select(left)
                                                          .from(table)
                                                          .where(left.notBetween(SQL.literalOf(1), SQL.literalOf(2)))
                                                          .build());

        assertThat(sql).endsWith("WHERE my_table.left NOT BETWEEN 1 AND 2");
    }
}
