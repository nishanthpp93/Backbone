package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SchemaUtilProxy {
    public static Schema toBeamSchema(ResultSetMetaData metadata) throws SQLException {
        return SchemaUtil.toBeamSchema(metadata);
    }

    public static class BeamRowMapperProxy implements JdbcIO.RowMapper<Row> {
        private SchemaUtil.BeamRowMapper proxied;

        public BeamRowMapperProxy(Schema schema) {
            this.proxied = SchemaUtil.BeamRowMapper.of(schema);
        }

        @Override
        public Row mapRow(ResultSet resultSet) throws Exception {
            return proxied.mapRow(resultSet);
        }
    }
}
