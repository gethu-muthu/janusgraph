// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop;

import com.google.common.collect.Iterables;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.janusgraph.graphdb.olap.job.IndexRepairJob;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractIndexManagementIT extends JanusGraphBaseTest {

    private void prepareGraphIndex() throws InterruptedException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Disable the "name" composite index
        JanusGraphManagement m = graph.openManagement();
        JanusGraphIndex nameIndex = m.getGraphIndex("name");
        m.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to DISABLED
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "name")
            .status(SchemaStatus.DISABLED).call().getSucceeded());
    }

    @Test
    public void testRemoveGraphIndex() throws InterruptedException, BackendException, ExecutionException {
        prepareGraphIndex();

        // Remove index
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        JanusGraphManagement m = graph.openManagement();
        JanusGraphIndex index = m.getGraphIndex("name");
        ScanMetrics metrics = mri.updateIndex(index, SchemaAction.DISCARD_INDEX).get();

        assertEquals(12, metrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Test
    public void testRemoveGraphIndexWithToolRunner() throws Exception {
        prepareGraphIndex();

        MapReduceRemoveIndexApp app = new MapReduceRemoveIndexApp(graph, "name");

        assertThrows(FileNotFoundException.class, () -> ToolRunner.run(app, new String[] {"-files", "invalid-file.txt"}));

        // submit the MapReduce job together with a dummy file
        ToolRunner.run(app, new String[] {"-files", getClass().getClassLoader().getResource("log4j2-test.xml").getPath()});
        assertEquals(12, app.getMetrics().getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Test
    public void testRemoveRelationIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Disable the "battlesByTime" index
        JanusGraphManagement m = graph.openManagement();
        RelationType battled = m.getRelationType("battled");
        RelationTypeIndex battlesByTime = m.getRelationIndex(battled, "battlesByTime");
        m.updateIndex(battlesByTime, SchemaAction.DISABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to DISABLED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "battlesByTime", "battled")
                .status(SchemaStatus.DISABLED).call().getSucceeded());

        // Remove index
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        battled = m.getRelationType("battled");
        battlesByTime = m.getRelationIndex(battled, "battlesByTime");
        ScanMetrics metrics = mri.updateIndex(battlesByTime, SchemaAction.DISCARD_INDEX).get();

        assertEquals(6, metrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

   @Test
   public void testRepairGraphIndex() throws InterruptedException, BackendException, ExecutionException {
        
        long timeoutMillis = 120000; // 2 minutes
        
        try {
            tx.commit();
            mgmt.commit();
            
            // Load the "Graph of the Gods" sample data (WITHOUT mixed index coverage)
            GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
            
            // Create and enable a graph index on age index with retry mechanism
            JanusGraphIndex index = createIndexWithRetry(timeoutMillis);
            
            // Wait until the SchemaStatus transitions to REGISTERED
            boolean statusReached = waitForIndexStatus(index, 
                SchemaStatus.REGISTERED, 
                timeoutMillis);
            assertTrue("Failed to reach REGISTERED status", statusReached);
            
            // Enable index with retry
            enableIndexWithRetry(index, timeoutMillis);
            
            // Wait for ENABLED status
            statusReached = waitForIndexStatus(index, 
                SchemaStatus.ENABLED, 
                timeoutMillis);
            assertTrue("Failed to reach ENABLED status", statusReached);
            
            // Verify initial query returns no results (as expected)
            assertFalse("Unexpected vertices found before repair", 
                graph.query().has("age", 10000).vertices().iterator().hasNext());
            
            // Perform index repair with detailed logging and retry
            ScanMetrics metrics = repairIndexWithRetry(index, timeoutMillis);
            
            assertNotNull("Metrics should not be null", metrics);
            assertEquals("Unexpected number of added records", 
                6, metrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));
            
            // Verify index works
            Iterable<JanusGraphVertex> hits = graph.query().has("age", 4500).vertices();
            assertNotNull("Vertex query result should not be null", hits);
            assertEquals("Unexpected number of vertices", 1, Iterables.size(hits));
            
            JanusGraphVertex v = Iterables.getOnlyElement(hits);
            assertNotNull("Vertex should not be null", v);
            assertEquals("Unexpected vertex name", "neptune", v.value("name"));
        } catch (Exception e) {
            // Log detailed error information
            logger.error("Test failed with exception", e);
            throw e;
        }
    }
    
    private JanusGraphIndex createIndexWithRetry(long timeoutMillis) throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            try {
                JanusGraphManagement m = graph.openManagement();
                PropertyKey age = m.getPropertyKey("age");
                JanusGraphIndex index = m.buildIndex("verticesByAge", Vertex.class)
                    .addKey(age)
                    .buildCompositeIndex();
                m.commit();
                graph.tx().commit();
                return index;
            } catch (Exception e) {
                Thread.sleep(1000); // Wait before retrying
            }
        }
        throw new TimeoutException("Failed to create index within timeout");
    }
    
    private boolean waitForIndexStatus(JanusGraphIndex index, 
                                       SchemaStatus status, 
                                       long timeoutMillis) throws Exception {
        return ManagementSystem.awaitGraphIndexStatus(graph, index.name())
            .status(status)
            .timeout(timeoutMillis, TimeUnit.MILLISECONDS)
            .call()
            .getSucceeded();
    }
    
    private ScanMetrics repairIndexWithRetry(JanusGraphIndex index, long timeoutMillis) throws Exception {
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            try {
                JanusGraphManagement m = graph.openManagement();
                m.updateIndex(index, SchemaAction.REINDEX);
                m.commit();
                graph.tx().commit();
                
                return mri.updateIndex(index, SchemaAction.REINDEX).get();
            } catch (Exception e) {
                Thread.sleep(1000); // Wait before retrying
            }
        }
        throw new TimeoutException("Failed to repair index within timeout");
    }

    @Test
    public void testRepairRelationIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data (WITHOUT mixed index coverage)
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Create and enable a relation index on lives edges by reason
        JanusGraphManagement m = graph.openManagement();
        PropertyKey reason = m.getPropertyKey("reason");
        EdgeLabel lives = m.getEdgeLabel("lives");
        m.buildEdgeIndex(lives, "livesByReason", Direction.BOTH, Order.desc, reason);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to REGISTERED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "livesByReason", "lives")
                .status(SchemaStatus.REGISTERED).call().getSucceeded());

        m = graph.openManagement();
        RelationTypeIndex index = m.getRelationIndex(m.getRelationType("lives"), "livesByReason");
        m.updateIndex(index, SchemaAction.ENABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to ENABLED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "livesByReason", "lives")
                .status(SchemaStatus.ENABLED).call().getSucceeded());

        // Run a query that hits the index but erroneously returns nothing because we haven't repaired yet
        //assertFalse(graph.query().has("reason", "no fear of death").edges().iterator().hasNext());

        // Repair
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        index = m.getRelationIndex(m.getRelationType("lives"), "livesByReason");
        ScanMetrics metrics = mri.updateIndex(index, SchemaAction.REINDEX).get();
        assertEquals(8, metrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));
    }
}
