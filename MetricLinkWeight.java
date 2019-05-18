/*
 * Copyright 2015-present Open Networking Foundation
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
 */

package org.onosproject.fwd;

import org.onlab.graph.ScalarWeight;
import org.onlab.graph.Weight;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.topology.LinkWeigher;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.fwd.ReactiveForwarding;
import static org.slf4j.LoggerFactory.getLogger;
import org.slf4j.Logger;

/**

 * Link weight for measuring link cost using the link metric annotation.
 */
class LinkWeight implements LinkWeigher  {

    private final Logger log = getLogger(getClass());
    @Override
    public Weight getInitialWeight() {
        return ScalarWeight.toWeight(0.0);
    }

    @Override
    public Weight getNonViableWeight() {
        return ScalarWeight.NON_VIABLE_WEIGHT;
    }

    @Override
    public Weight weight(TopologyEdge edge) {
        float w = ReactiveForwarding.link_Weight.get(edge.src().toString()+"-"+edge.dst().toString());
        //log.info("\n********************Edge: {}, Weight:{} ********************\n",edge, w);
        //for(String s: ReactiveForwarding.link_Weight.keySet()){
        //    log.info("\n********************Edge:{}, Weight: {} ********************\n",edge, ReactiveForwarding.link_Weight.get(s));
        //}
        String v = edge.link().annotations().value(AnnotationKeys.METRIC);
        try {
            return ScalarWeight.toWeight(v != null ? Double.parseDouble(v) : w);
        } catch (NumberFormatException e) {
            return ScalarWeight.toWeight(1.0);
        }
    }
}

