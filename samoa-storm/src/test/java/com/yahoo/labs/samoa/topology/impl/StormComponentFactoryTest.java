package com.yahoo.labs.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
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

import org.junit.Before;
import org.junit.Test;

import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;

import mockit.Mocked;
import mockit.Tested;

/**
 *  
 * @author arinto
 *
 */
public class StormComponentFactoryTest {
    
    @Tested private StormComponentFactory factory;
    @Mocked private Processor processor;
    @Mocked private EntranceProcessor entranceProcessor;
    
    private final int parallelism = 3;
    private final String topoName = "testTopology";
    
    @Before
    public void setUp() throws Exception {
        factory = new StormComponentFactory();
    }
    
    @Test
    public void testCreatePiNoParallelism() {
        ProcessingItem pi = factory.createPi(processor);
        assertNotNull("ProcessingItem created is null.", pi);
        assertEquals("ProcessingItem created is not a StormProcessingItem.",StormProcessingItem.class,pi.getClass());
        assertEquals("Parallelism of PI is not 1.", 1, pi.getParallelism(), 0);
    }
    
    @Test
    public void testCreatePiWithParallelism() {
        ProcessingItem pi = factory.createPi(processor, parallelism);
        assertNotNull("ProcessingItem created is null.", pi);
        assertEquals("ProcessingItem created is not a StormProcessingItem.",StormProcessingItem.class,pi.getClass());
        assertEquals(String.format("Parallelism of PI is not %d.", parallelism), parallelism, pi.getParallelism(), 0);
    }
    
    @Test
    public void testCreateStream() {
        ProcessingItem pi = factory.createPi(processor);
        Stream stream = factory.createStream(pi);
        assertNotNull("Stream created is null.", stream);
        assertTrue("Stream created is not StormStream", StormStream.class.isAssignableFrom(stream.getClass()));
        assertEquals("Stream created is not StormBoltStream", StormBoltStream.class, stream.getClass());
        
        EntranceProcessingItem epi = factory.createEntrancePi(entranceProcessor);
        Stream entranceStream = factory.createStream(epi);
        assertNotNull("Stream created is null.", entranceStream);
        assertTrue("Stream created is not StormStream", StormStream.class.isAssignableFrom(entranceStream.getClass()));
        assertEquals("Stream created is not StormBoltStream", StormBoltStream.class, entranceStream.getClass());
    }
    
    @Test
    public void testCreateTopology() {
        Topology topology = factory.createTopology(topoName);
        assertNotNull("Topology created is null.", topology);
        assertEquals("Topology created is not StormTopology", StormTopology.class, topology.getClass());
        assertEquals("Topology name is not correct", topology.getTopologyName(), topoName);
    }
    
    @Test
    public void testCreateEntrancePI() {
        EntranceProcessingItem entrancePi = factory.createEntrancePi(entranceProcessor);
        assertNotNull("EntranceProcessingItem created is null.", entrancePi);
        assertEquals("EntranceProcessingItem created is not a StormProcessingItem.",StormEntranceProcessingItem.class,entrancePi.getClass());
    }

}
