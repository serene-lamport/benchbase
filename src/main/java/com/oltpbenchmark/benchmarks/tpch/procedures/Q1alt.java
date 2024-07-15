/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.toEpochDate;

public class Q1alt extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
              SELECT
                 l_returnflag,
                 l_linestatus,
                 SUM(l_quantity) AS sum_qty,
                 SUM(l_extendedprice) AS sum_base_price,
                 SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                 SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                 AVG(l_quantity) AS avg_qty,
                 AVG(l_extendedprice) AS avg_price,
                 AVG(l_discount) AS avg_disc,
                 COUNT(*) AS count_order
              FROM
                 lineitem
              WHERE
                 l_shipdate between ? and ?
              GROUP BY
                 l_returnflag,
                 l_linestatus
              ORDER BY
                 l_returnflag,
                 l_linestatus
            """
    );

    public final SQLStmt query_stmt_nofilter = new SQLStmt("""
              SELECT
                 l_returnflag,
                 l_linestatus,
                 SUM(l_quantity) AS sum_qty,
                 SUM(l_extendedprice) AS sum_base_price,
                 SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                 SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                 AVG(l_quantity) AS avg_qty,
                 AVG(l_extendedprice) AS avg_price,
                 AVG(l_discount) AS avg_disc,
                 COUNT(*) AS count_order
              FROM
                 lineitem
              GROUP BY
                 l_returnflag,
                 l_linestatus
              ORDER BY
                 l_returnflag,
                 l_linestatus
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor, double selectivity) throws SQLException {
        PreparedStatement stmt;
        int day_range;

        // only take the fractional part of selectivity
        if (selectivity > 1.) {
            selectivity = selectivity % 1.;
        }

        // order date is from 92001 to 94406 (inclusive) => 2406 day range
        if (selectivity > 0) {
            // If selectivity is specified, use that as the day range for all queries
            day_range = (int) (selectivity * 2406);
        } else {
            // Otherwise, pick from {1%, 10%, 50%, 100%}
            int type = rand.number(1, 4);
            day_range = switch (type) {
                case 1 -> 24; // 1% of rows
                case 2 -> 241; // 10% of rows
                case 3 -> 1203; // 50% of rows
                default -> 0;

            };

            // type 4 => 100% of rows. Return query without WHERE clause.
            if (type == 4) {
                return this.getPreparedStatement(conn, query_stmt_nofilter);
            }
        }

        // generate random shipping date as the start
        // generate the same way as the loader (order date first, then random delta) to ensure the same distribution
        stmt = this.getPreparedStatement(conn, query_stmt);
        int odi = rand.number(OrderGenerator.ORDER_DATE_MIN, OrderGenerator.ORDER_DATE_MAX - day_range);
        int sdi_lo = odi + rand.number(LineItemGenerator.SHIP_DATE_MIN, LineItemGenerator.SHIP_DATE_MAX);
        int sdi_hi = sdi_lo + day_range;

        Date sd_lo = toEpochDate(sdi_lo);
        Date sd_hi = toEpochDate(sdi_hi);

        stmt.setDate(1, sd_lo);
        stmt.setDate(2, sd_hi);

        return stmt;
    }
}
