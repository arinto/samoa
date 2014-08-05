package com.yahoo.labs.samoa.topology.impl;

import java.util.Arrays;
import java.util.Collection;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Tested;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.impl.StormProcessingItem.ProcessingItemBolt;
import com.yahoo.labs.samoa.utils.PartitioningScheme;

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

/**
 * 
 * @author arinto
 *
 */

@RunWith(Parameterized.class)
public class StormBoltStreamTest {
    
    @Tested private StormBoltStream stream;
    
    @Mocked private StormProcessingItem sourcePi, destPi;
    @Mocked private ContentEvent event;
    @Mocked private OutputCollector outputCollector;
    @Mocked private ProcessingItemBolt piBolt;
    @Mocked private Processor proc;
    
    @Mocked private StormComponentFactory scf;
    
    private final int parallelism;
    private final PartitioningScheme scheme;
    
    @Parameters 
    public static Collection<Object[]> generateParameters() {
        return Arrays.asList(new Object[][] {
                {2, PartitioningScheme.SHUFFLE},
                {3, PartitioningScheme.GROUP_BY_KEY},
                {4, PartitioningScheme.BROADCAST}
        });
    }
    
    public StormBoltStreamTest(int parallelism, PartitioningScheme scheme) {
        this.parallelism = parallelism;
        this.scheme = scheme;
    }
    
    @Before
    public void setUp() {
        this.scf = new StormComponentFactory();
        this.sourcePi = (StormProcessingItem) scf.createPi(proc, parallelism);
        this.stream = (StormBoltStream) scf.createStream(sourcePi);
    }
    
    @Test
    public void testPut() {
        
        //TODO: check what expectations to use
        new NonStrictExpectations() {
            {
                outputCollector.emit(anyString, new Values(event, event.getKey()));
            }
        };
        switch(this.scheme) {
        case SHUFFLE:
            this.destPi.connectInputShuffleStream(stream);
            this.piBolt = destPi.getBolt();
            //TODO: read more on Mockito on how to handle this case
            new Expectations() {
                {
                    piBolt.execute((Tuple) any); times = 1; 
                }
            };
            break;
        case GROUP_BY_KEY:
            this.destPi.connectInputKeyStream(stream);
            this.piBolt = destPi.getBolt();
            new Expectations() {
                {
                    piBolt.execute((Tuple) any); times = 1;
                }
            };
            break;
        case BROADCAST:
            this.destPi.connectInputAllStream(stream);
            this.piBolt = destPi.getBolt();
            new Expectations() {
                {
                    piBolt.execute((Tuple) any); times = parallelism;
                }
            };
            break;
        }
        stream.put(event);
    }
    
}
