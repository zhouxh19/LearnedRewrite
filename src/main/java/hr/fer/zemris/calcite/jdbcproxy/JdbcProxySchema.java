package hr.fer.zemris.calcite.jdbcproxy;

import org.apache.calcite.adapter.jdbc.JdbcSchema;

import javax.sql.DataSource;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schema;

import java.util.Map;

class JdbcProxySchema extends JdbcSchema {

	private JdbcProxySchema(
			DataSource dataSource,
			SqlDialect dialect,
			JdbcConvention convention,
			String catalog,
			String schema,
			Map<String, Object> operand) {

		super(dataSource, dialect, convention, catalog, schema);
	}

	public static class Factory implements SchemaFactory {
		public static final Factory INSTANCE = new Factory();

                private Factory() {
                }

                public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
                        return JdbcProxySchema.create(parentSchema, name, operand);
                }
        }
}
