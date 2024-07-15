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

package com.oltpbenchmark.benchmarks.tpch;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpch.procedures.GenericQuery;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.SQLException;

public class TPCHWorker extends Worker<TPCHBenchmark> {

    private final RandomGenerator rand;
    private final double scaleFactor;
    private final double selectivity;

    public TPCHWorker(TPCHBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        // "this.rng()" will be the same generator for each worker, since initialized from the same thread.
        // We want each thread's seed to be dependent (only) on the main thread seed (if specified, for repeatable
        // tests) but not the same for each worker!
        rand = new RandomGenerator(this.rng().nextInt() + 15721 * id);
        this.scaleFactor = this.configuration.getScaleFactor();
        this.selectivity = this.configuration.getSelectivity();
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, rand, this.scaleFactor, this.selectivity);
        } catch (ClassCastException e) {
            throw new RuntimeException(e);
        }

        return (TransactionStatus.SUCCESS);

    }
}

