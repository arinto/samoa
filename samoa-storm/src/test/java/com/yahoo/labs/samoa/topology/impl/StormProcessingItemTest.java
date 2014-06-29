package com.yahoo.labs.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.junit.Assert.*;
import mockit.Expectations;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.impl.StormProcessingItem.ProcessingItemBolt;
import com.yahoo.labs.samoa.topology.impl.StormStream.InputStreamId;

public class StormProcessingItemTest {
    private static final int PARALLELISM_HINT_2 = 2;
    private static final int PARALLELISM_HINT_4 = 4;
    private static final String ID = "id";
    @Tested private StormProcessingItem pi;
    @Mocked private StormBoltStream stormInputStream;
    @Mocked private Processor processor;
    @Mocked private TopologyBuilder stormBuilder = new TopologyBuilder();
    @Mocked private BoltDeclarer piBoltDeclarer;
    @Mocked private ProcessingItemBolt piBolt;
    @Mocked private StormTopology topology;

    private final InputStreamId inputId = new InputStreamId("compId", "streamId");

    @Before
    public void setUp() {
        pi = new StormProcessingItem(processor, ID, PARALLELISM_HINT_2);
    }

    @Test
    public void testConstructor() {
        assertSame("Processor is not same", pi.getProcessor(), processor);
        assertEquals("Parallelism is not set correctly", pi.getParallelism(), PARALLELISM_HINT_2 ,0);
    }
    
    @Test
    public void testAddToTopology() {
        new Expectations() {
            {
                topology.getStormBuilder();
                result = stormBuilder;

                stormBuilder.setBolt(ID, (IRichBolt) any, anyInt);
                result = new MockUp<BoltDeclarer>() {
                }.getMockInstance();
            }
        };

        pi.addToTopology(topology, PARALLELISM_HINT_4); // this parallelism hint is ignored

        new Verifications() {
            {
                assertEquals(pi.getProcessor(), processor);
                // TODO add methods to explore a topology and verify them
                assertEquals(pi.getParallelism(), PARALLELISM_HINT_2);
                assertEquals(pi.getId(), ID);
            }
        };
    }
    
    @Test
    public void testCreateStream(){
        new Expectations() {
            {
                piBolt.createStream(ID);
            }
        };
        
        pi.createStream();
    }
    
//    @Test
    public void testConnectInputShuffleStream() {   
        StormComponentFactory scf = new StormComponentFactory();
        StormTopology topo = (StormTopology)scf.createTopology("topoName");
        topo.addProcessingItem(pi);
        
        new Expectations() {
            {
                stormInputStream.getInputId(); result = inputId;
                piBoltDeclarer.shuffleGrouping(inputId.getComponentId(), 
                        inputId.getStreamId());
            }
        };
        pi.connectInputShuffleStream(stormInputStream);
    }
    
//    @Test
    public void testConnectInputKeyStream() {
        StormComponentFactory scf = new StormComponentFactory();
        StormTopology topo = (StormTopology) scf.createTopology("topoName");
        topo.addProcessingItem(pi);

        new Expectations() {
            {
                stormInputStream.getInputId(); result = inputId;
                piBoltDeclarer.fieldsGrouping(
                        inputId.getComponentId(), 
                        inputId.getStreamId(), 
                        new Fields(StormSamoaUtils.KEY_FIELD));            }
        };
        
        pi.connectInputKeyStream(stormInputStream);
    }
    
//    @Test
    public void testConnectAllStream() {
        StormComponentFactory scf = new StormComponentFactory();
        StormTopology topo = (StormTopology) scf.createTopology("topoName");
        topo.addProcessingItem(pi);

        new Expectations() {
            {
                stormInputStream.getInputId(); result = inputId;
                piBoltDeclarer.allGrouping(
                        inputId.getComponentId(), 
                        inputId.getStreamId());
            }
        };
        
        pi.connectInputAllStream(stormInputStream);
    }
}
