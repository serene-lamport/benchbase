package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.util.SupplierGenerator;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class QMicro3 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT max(ep), max(ad)
            FROM (
              SELECT l_suppkey, sum(l_extendedprice) AS ep, avg(l_discount) AS ad
              FROM lineitem
                WHERE l_suppkey BETWEEN ? AND ?
              GROUP BY l_suppkey
            ) sub;
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor, double selectivity) throws SQLException {
        PreparedStatement stmt;

        // extract integral part of "selectivity" to determine over-all range (= percentage of table)
        double range_mult = ((int)selectivity / 100.);

        // only take the fractional part for the actual selectivity
        selectivity = selectivity % 1.;

        // maximum supplier key in the dataset
        int max_suppkey = (int) (SupplierGenerator.SCALE_BASE * scaleFactor);

        // difference between lower and upper bound of the scan range
        int suppkey_delta = (int) (selectivity * max_suppkey);

        // maximum supplier key which will be queried (assuming range_mult >= selectivity)
        int q_max_suppkey = (int)(max_suppkey * range_mult);

        // pick lower & upper bounds for the query
        int skey_lo = rand.number(1, Math.max(q_max_suppkey - suppkey_delta, 1));
        int skey_hi = skey_lo + suppkey_delta;

//        LOG.warn("lo={}, hi={}  |  delta={}, q_max={}, max={}", skey_lo, skey_hi, suppkey_delta, q_max_suppkey, max_suppkey);

        stmt = this.getPreparedStatement(conn, query_stmt);

        stmt.setInt(1, skey_lo);
        stmt.setInt(2, skey_hi);

        return stmt;
    }
}
