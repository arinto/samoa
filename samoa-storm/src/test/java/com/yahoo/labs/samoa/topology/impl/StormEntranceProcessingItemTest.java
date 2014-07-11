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

import backtype.storm.topology.IRichSpout;

import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;

/**
 * 
 * @author arinto
 *
 */
public class StormEntranceProcessingItemTest {
    
    @Tested private StormEntranceProcessingItem stormEntrancePi;
    
    @Mocked private EntranceProcessor entranceProcessor;
    private Stream outputStream, anotherStream;
    @Mocked private Stream createdStream;
    @Mocked private Topology topology =  new StormComponentFactory().createTopology("mockTopo");
    
    @Before
    public void setUp() {
        this.stormEntrancePi = new StormEntranceProcessingItem(entranceProcessor);
    }
    
    @Test
    public void testContructoWithRandomUUID() {
        assertSame("EntranceProcessor is not set correctly.",entranceProcessor,stormEntrancePi.getProcessor());
    }
    
    @Test
    public void testContructoWithFriendlyID() {
        String friendlyID = "friendly";
        stormEntrancePi = new StormEntranceProcessingItem(this.entranceProcessor, friendlyID);
        assertSame("EntranceProcessor is not set correctly.",entranceProcessor,stormEntrancePi.getProcessor());
        assertEquals("FriendlyID is not set correctly", friendlyID, stormEntrancePi.getId());
    }
    
    @Test
    public void testSetOutputStream() {
        outputStream = new StormComponentFactory().createStream(stormEntrancePi);
        stormEntrancePi.setOutputStream(outputStream);
        assertSame("OutputStream is not set correctly.",outputStream,stormEntrancePi.getOutputStream());
    }
    
    @Test
    public void testSetOutputStreamRepeat() {
        outputStream = new StormComponentFactory().createStream(stormEntrancePi);
        stormEntrancePi.setOutputStream(outputStream);
        stormEntrancePi.setOutputStream(outputStream);
        assertSame("OutputStream is not set correctly.",outputStream,stormEntrancePi.getOutputStream());
    }
    
    @Test(expected=IllegalStateException.class)
    public void testSetOutputStreamError() {
        StormComponentFactory sfc = new StormComponentFactory();
        outputStream = sfc.createStream(stormEntrancePi);
        anotherStream = sfc.createStream(stormEntrancePi);
        
        stormEntrancePi.setOutputStream(outputStream);
        stormEntrancePi.setOutputStream(anotherStream);
    }
    
    @Test
    public void testCreateStream() {
        String friendlyID = "friendly";
        stormEntrancePi = new StormEntranceProcessingItem(this.entranceProcessor, friendlyID);
        assertSame("EntranceProcessor is not set correctly.",entranceProcessor,stormEntrancePi.getProcessor());
        assertEquals("FriendlyID is not set correctly", friendlyID, stormEntrancePi.getId());
    }
    
    @Test
    public void testAddToTopology() {
        final String friendlyID = "friendly";
        stormEntrancePi = new StormEntranceProcessingItem(this.entranceProcessor, friendlyID);
        final IRichSpout spout = stormEntrancePi.getSpout();
        final StormTopology stormTopo = (StormTopology) topology;
        new Expectations() {
            {
                stormTopo.getStormBuilder().setSpout(anyString, spout, anyInt);
            }
        };
        
        stormEntrancePi.addToTopology(stormTopo, 3);
    }    
}
