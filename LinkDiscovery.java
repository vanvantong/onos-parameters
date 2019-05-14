/*
 * Copyright 2016-present Open Networking Foundation
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
package org.onosproject.provider.lldpcommon;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.internal.StringUtil;
import org.onlab.packet.Ethernet;
import org.onlab.packet.MacAddress;
import org.onlab.packet.ONOSLLDP;
import org.onlab.util.Timer;
import org.onlab.util.Tools;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link.Type;
import org.onosproject.net.LinkKey;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.link.DefaultLinkDescription;
import org.onosproject.net.link.LinkDescription;
import org.onosproject.net.link.ProbedLinkProvider;
import org.onosproject.net.packet.DefaultOutboundPacket;
import org.onosproject.net.packet.OutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.onosproject.net.AnnotationKeys.PORT_NAME;
import static org.onosproject.net.PortNumber.portNumber;
import static org.onosproject.net.flow.DefaultTrafficTreatment.builder;
import static org.slf4j.LoggerFactory.getLogger;
import java.util.Arrays;

import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.lang.*;
import java.util.*;
import org.onosproject.net.device.PortStatistics;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Run discovery process from a physical switch. Ports are initially labeled as
 * slow ports. When an LLDP is successfully received, label the remote port as
 * fast. Every probeRate milliseconds, loop over all fast ports and send an
 * LLDP, send an LLDP for a single slow port. Based on FlowVisor topology
 * discovery implementation.
 */
public class LinkDiscovery implements TimerTask {

    private static final String SCHEME_NAME = "linkdiscovery";
    private static final String ETHERNET = "ETHERNET";

    private final Logger log = getLogger(getClass());

    private final DeviceId deviceId;
    private final LinkDiscoveryContext context;

    private final Ethernet ethPacket;
    private final Ethernet bddpEth;

    private Timeout timeout;
    private volatile boolean isStopped;

    ArrayList<Timestamp> timePara = new ArrayList<Timestamp>();
    // Initializing a dictionary of link delay
    Map<String, ArrayList<Float>> linkDelay = new HashMap<String, ArrayList<Float>>();
    Map<String, ArrayList<Float>> linkPacketLoss = new HashMap<String, ArrayList<Float>>();
    Map<String, ArrayList<Float>> linkRate = new HashMap<String, ArrayList<Float>>(); 



    float link_capacity = 25;
    int threshold_packet_loss = 1000;

    public int arrSize = 1000;

    Map<String, Integer> countPara = new HashMap<String, Integer>();


    // Set of ports to be probed
    private final Map<Long, String> portMap = Maps.newConcurrentMap();
    /**
     * Instantiates discovery manager for the given physical switch. Creates a
     * generic LLDP packet that will be customized for the port it is sent out on.
     * Starts the the timer for the discovery process.
     *
     * @param deviceId  the physical switch
     * @param context discovery context
     */
    public LinkDiscovery(DeviceId deviceId, LinkDiscoveryContext context) {
        this.deviceId = deviceId;
        this.context = context;

        ethPacket = new Ethernet();
        ethPacket.setEtherType(Ethernet.TYPE_LLDP);
        ethPacket.setDestinationMACAddress(MacAddress.ONOS_LLDP);
        ethPacket.setPad(true);

        bddpEth = new Ethernet();
        bddpEth.setEtherType(Ethernet.TYPE_BSN);
        bddpEth.setDestinationMACAddress(MacAddress.BROADCAST);
        bddpEth.setPad(true);

        isStopped = true;
        start();
        log.debug("Started discovery manager for switch {}", deviceId);

    }

    public synchronized void stop() {
        if (!isStopped) {
            isStopped = true;
            timeout.cancel();
        } else {
            log.warn("LinkDiscovery stopped multiple times?");
        }
    }

    public synchronized void start() {
        if (isStopped) {
            isStopped = false;
            timeout = Timer.newTimeout(this, 0, MILLISECONDS);
        } else {
            log.warn("LinkDiscovery started multiple times?");
        }
    }

    public synchronized boolean isStopped() {
        return isStopped || timeout.isCancelled();
    }

    /**
     * Add physical port to discovery process.
     * Send out initial LLDP and label it as slow port.
     *
     * @param port the port
     */
    public void addPort(Port port) {
        Long portNum = port.number().toLong();
        String portName = port.annotations().value(PORT_NAME);
        if (portName == null) {
            portName = StringUtil.EMPTY_STRING;
        }

        boolean newPort = !containsPort(portNum);
        portMap.put(portNum, portName);

        boolean isMaster = context.mastershipService().isLocalMaster(deviceId);
        if (newPort && isMaster) {
            log.debug("Sending initial probe to port {}@{}", port.number().toLong(), deviceId);
            sendProbes(portNum, portName);
        }
    }

    /**
     * removed physical port from discovery process.
     * @param port the port number
     */
    public void removePort(PortNumber port) {
        portMap.remove(port.toLong());
    }

    /**
     * Handles an incoming LLDP packet. Creates link in topology and adds the
     * link for staleness tracking.
     *
     * @param packetContext packet context
     * @return true if handled
     */
    public boolean handleLldp(PacketContext packetContext) {
        Ethernet eth = packetContext.inPacket().parsed();
        if (eth == null) {
            return false;
        }

        if (processOnosLldp(packetContext, eth)) {
            return true;
        }

        if (processLldp(packetContext, eth)) {
            return true;
        }

        ONOSLLDP lldp = ONOSLLDP.parseLLDP(eth);

        if (lldp == null) {
            log.debug("Cannot parse the packet. It seems that it is not the lldp or bsn packet.");
        } else {
            log.debug("LLDP packet is dropped due to there are no handlers that properly handle this packet: {}",
                    lldp.toString());
        }

        return false;
    }

    /*
     * Write the network parameters to file
     * 
     *
     * @param nFile  The file name for storing network parameters
     * @param id_port  The port id of switch observed
     * @return true if handled
     */
    public boolean writeToFile(String nFile, String id_port){
        try {
            // If the number of parameters in array is equal to arrSize, use the threshold to create the original array
            BufferedWriter writer = new BufferedWriter(new FileWriter(nFile));
            if(linkDelay.get(id_port).size() == arrSize){
                //log.info("Delay 2: {}", linkDelay.get(id_port).toString());
                //log.info("Packet loss 2: {}", linkPacketLoss.get(id_port).toString());
                //log.info("Rate 2: {}", linkRate.get(id_port).toString());
                int threshold = countPara.get(id_port) + 1;
                String de_link=String.valueOf(linkDelay.get(id_port).get(threshold));
                String pl_link = String.valueOf(linkPacketLoss.get(id_port).get(threshold));
                String r_link = String.valueOf(linkRate.get(id_port).get(threshold));

                for(int i = threshold + 1; i < arrSize; i++){
                    de_link = de_link + "," + String.valueOf(linkDelay.get(id_port).get(i));
                    pl_link = pl_link + "," + String.valueOf(linkPacketLoss.get(id_port).get(i));
                    r_link = r_link + "," + String.valueOf(linkRate.get(id_port).get(i));
                }
                for(int i = 0; i < threshold; i++){
                    de_link = de_link + "," + String.valueOf(linkDelay.get(id_port).get(i));
                    pl_link = pl_link + "," + String.valueOf(linkPacketLoss.get(id_port).get(i));
                    r_link = r_link + "," + String.valueOf(linkRate.get(id_port).get(i));
                }
                writer.write(de_link+"\n");
                writer.write(pl_link+"\n");
                writer.write(r_link+"\n");

            }else{
                //log.info("Delay 1: {}", linkDelay.get(id_port).toString());
                //log.info("Packet loss 1: {}", linkPacketLoss.get(id_port).toString());
                //log.info("Rate 1: {}", linkRate.get(id_port).toString());
                String de_link=String.valueOf(linkDelay.get(id_port).get(0));
                String pl_link = String.valueOf(linkPacketLoss.get(id_port).get(0));
                String r_link = String.valueOf(linkRate.get(id_port).get(0));
                int count_pa = linkDelay.get(id_port).size();
                for(int i = 1; i < count_pa; i++){
                    de_link = de_link + "," + String.valueOf(linkDelay.get(id_port).get(i));
                    pl_link = pl_link + "," + String.valueOf(linkPacketLoss.get(id_port).get(i));
                    r_link = r_link + "," + String.valueOf(linkRate.get(id_port).get(i));
                }
                writer.write(de_link+"\n");
                writer.write(pl_link+"\n");
                writer.write(r_link+"\n");
            }

            writer.close();
            

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /*
     * Calculate the breakpoint on each link
     * 
     *
     * @param id  The id of port
     * @param cts  The current timestamp
     * @param delay_link  The delay of link
     * @param packet_loss  The packet loss on each link
     * @param rate_link  The rateRx/rateTx     
     * @return true if handled
     */
    public boolean breakPointCal(String id, Timestamp cts, float delay_link, float packet_loss, float rate_link){

        if(linkDelay.containsKey(id)){
            int tmp = countPara.get(id);
            if(tmp < arrSize){
                if(linkDelay.get(id).size() != arrSize){
                    linkDelay.get(id).add(delay_link);
                    linkPacketLoss.get(id).add(packet_loss);
                    linkRate.get(id).add(rate_link);
                }else{
                    linkDelay.get(id).set(tmp, delay_link);
                    linkPacketLoss.get(id).set(tmp, packet_loss);
                    linkRate.get(id).set(tmp, rate_link);
                }
                            
            }else{
                countPara.replace(id, tmp - arrSize);
                linkDelay.get(id).set(countPara.get(id), delay_link);
                linkPacketLoss.get(id).set(countPara.get(id), packet_loss);
                linkRate.get(id).set(countPara.get(id), rate_link);
            }
            countPara.replace(id, countPara.get(id) + 1);
        }else{
            linkDelay.put(id, new ArrayList<Float>());
            linkPacketLoss.put(id, new ArrayList<Float>());
            linkRate.put(id, new ArrayList<Float>());

            linkDelay.get(id).add(delay_link);
            linkPacketLoss.get(id).add(packet_loss);
            linkRate.get(id).add(rate_link);
            countPara.put(id, 1);
        }

        log.info("\nPort: {}, Size of link delay: {}, Array: {}, Count: {}\n", id, linkDelay.get(id).size(), linkDelay.get(id).toString(), countPara.get(id));
        log.info("\nPort: {}, Size of packet loss: {}, Array: {}, Count: {}\n", id, linkPacketLoss.get(id).size(), linkPacketLoss.get(id).toString(), countPara.get(id));
        log.info("\nPort: {}, Size of link rate: {}, Array: {}, Count: {}\n", id, linkRate.get(id).size(), linkRate.get(id).toString(), countPara.get(id));
        log.info("Size: {}", linkDelay.get(id).size() );


        if(linkDelay.get(id).size() >= 20){
            String directory = "/home/vantong/onos/providers/lldpcommon/src/main/java/org/onosproject/provider/lldpcommon/data-" +id + ".csv";
            boolean wFile = writeToFile(directory, id);            
            /*
            BufferedReader reader = null;
            Process shell = null;
            try {
                shell = Runtime.getRuntime().exec(new String[] { "Rscript", "/home/vantong/onos/providers/lldpcommon/src/main/java/org/onosproject/provider/lldpcommon/BreakPointDetection.R" });
                reader = new BufferedReader(new InputStreamReader(shell.getInputStream()));
                BufferedWriter wr= new BufferedWriter(new FileWriter("/home/vantong/onos/providers/lldpcommon/src/main/java/org/onosproject/provider/lldpcommon/Result.csv"));

                String line;
                while ((line = reader.readLine()) != null) {
                    String s[] = line.split(" ");
                    log.info(s[1]);
                    wr.write(s[1]+"\n");                 
                }
                wr.close();
                reader.close();
                
            } catch (Exception e) {
                e.printStackTrace();
            }
            */
        }




        return true;
    }




    /*
     * Calculate the delay on each link
     * 
     *
     * @param onoslldpDelay  The lldp packet on each link
     * @return the link delay
     */
    public long delayCal(ONOSLLDP onoslldpDelay, String id){

        //Calculate the link's delay using lldp
        Timestamp current_timestamp = new Timestamp(System.currentTimeMillis());
        ByteBuffer str_timestamp_tmp = ByteBuffer.allocate(8).put(onoslldpDelay.getTimestampTLV().getInfoString());
        long current_ts_nano = (current_timestamp.getTime());
        str_timestamp_tmp.flip();
        long lldp_ts_nano = str_timestamp_tmp.getLong();
        long delay = current_ts_nano - lldp_ts_nano;

        //Remove the timestamp when the lldp packet reaches the controller
        ONOSLLDP.removeElement(id, lldp_ts_nano);
        ArrayList<Long> arrPacketLossLLDP = ONOSLLDP.getArray(id);

        //log.info("Removing the timestamp of {} : {}", id, lldp_ts_nano);

        int count_packet_loss = 0;
        if(arrPacketLossLLDP.size() != 0){
            for(int i = 0; i < arrPacketLossLLDP.size(); i++){
                if(current_ts_nano - arrPacketLossLLDP.get(i) > threshold_packet_loss){
                    count_packet_loss  = count_packet_loss + 1;
                    ONOSLLDP.removeElement(id, arrPacketLossLLDP.get(i));
                }
                
            }

        }
        log.info("\nPackets loss of LLDP of {}: {}\n", id, count_packet_loss);

        return delay;
    }


    /*
     * Calculate the statistic on each port of switch
     * 
     *
     * @param deviceId  Device Id of the switch
     * @param sPort  The port which need to monitor 
     * @return true if handled
     */
    public String statisticsCal(DeviceId deviceId, PortNumber sPort, DeviceId dDeviceId, PortNumber dPort){

        //Calculate the statistic
        DeviceService deviceService = context.deviceService();
        Device d = deviceService.getDevice(deviceId);
        List<PortStatistics> portStatisticsList =  deviceService.getPortStatistics(d.id());

        float packetLoss = 0;
        float link_utilization = 0;

        //Null point exception
        link_utilization = (deviceService.getStatisticsForPort(deviceService.getDevice(deviceId).id(), sPort).packetsSent() + deviceService.getStatisticsForPort(deviceService.getDevice(dDeviceId).id(), dPort).packetsSent())/(link_capacity * 1000000);
        float pSent_Src = deviceService.getStatisticsForPort(deviceService.getDevice(deviceId).id(), sPort).packetsSent();
        float pReceived_Dest = deviceService.getStatisticsForPort(deviceService.getDevice(deviceId).id(), dPort).packetsReceived();
        log.info("\nId {}:{} with {} sent and {} received\n", deviceId, sPort, pSent_Src, pReceived_Dest);
        if(pReceived_Dest > 0){
            packetLoss = (pSent_Src - pReceived_Dest)/pSent_Src;
        }else{
            packetLoss = 0;
        }
        for (PortStatistics portStats : portStatisticsList) {   
            if(portStats.portNumber().toLong() == sPort.toLong()){
                float duration = (float) portStats.durationSec();
                float rateRx = duration > 0 ? portStats.bytesReceived() * 8 / duration : 0;
                float rateTx = duration > 0 ? portStats.bytesSent() * 8 / duration : 0;
                log.info("\n\n{}:{} received {} bytes, {} packets, rateReceiver {} bps, drop {}, {} and sent {} bytes, {} packets, rateSender {} bps, drop {}, {}  with time interval of {} s\n\n", deviceId, portStats.portNumber(), portStats.bytesReceived(), portStats.packetsReceived(), rateRx, portStats.packetsRxDropped(), portStats.packetsRxErrors(), portStats.bytesSent(),  portStats.packetsSent(), rateTx, portStats.packetsTxDropped(), portStats.packetsTxErrors() , duration);
                /*
                if(portStats.packetsReceived() > 0){
                    float packetLoss =  ((float)portStats.packetsSent() - (float)portStats.packetsReceived()) / (float)portStats.packetsSent();
                    float rate = (float)rateRx/(float)rateTx;
                    String s = String.valueOf(packetLoss)+","+String.valueOf(rate);

                    return s;
                }
                */
            }
        
        }

        
        /*
        float duration = 0;
        float rateRx = 0;
        float rateTx = 0;
        float pReceived = 0;
        float pSent = 0;
        float rate = 0;
        float kbReceived_Id = 0;
        float kbSent_Id = 0;
        for (PortStatistics portStats : portStatisticsList) {
            duration = duration + (float) portStats.durationSec();
            float rateRx_tmp = duration > 0 ? portStats.bytesReceived() / (duration * 1000) : 0;
            float rateTx_tmp = duration > 0 ? portStats.bytesSent() / (duration * 1000) : 0;
            rateRx = rateRx + (float)rateRx_tmp;
            rateTx = rateTx + (float)rateTx_tmp;
            pReceived = pReceived + (float)portStats.packetsReceived();
            pSent = pSent + (float)portStats.packetsSent();
            kbReceived_Id = kbReceived_Id + (float)portStats.bytesReceived();
            kbSent_Id = kbSent_Id + (float)portStats.bytesSent();
        }
        if(portStatisticsList.size() > 0){
            int size = portStatisticsList.size();
            duration = duration / size;
            pReceived = pReceived / size;
            pSent = pSent / size;
            rate = pReceived / pSent;
            kbReceived_Id = kbReceived_Id / (size * 1000);
            kbSent_Id = kbSent_Id / (size * 1000);
            log.info("\n\n{}:{} received {} Kbytes, {} packets, rateReceiver {} KBps and sent {} Kbytes, {} packets, rateSender {} KBps, link utilization: {}  with time interval of {} s\n\n", deviceId, sPort, kbReceived_Id, pReceived, rateRx/size, kbSent_Id,  pSent, rateTx/size, link_utilization, duration);
            packetLoss = (pReceived - pSent)/pReceived;
            String s = String.valueOf(packetLoss)+","+String.valueOf(rate);
            return s;
        }

        */



        return String.valueOf(packetLoss)+","+String.valueOf(link_utilization);
    } 
    private boolean processOnosLldp(PacketContext packetContext, Ethernet eth) {
        ONOSLLDP onoslldp = ONOSLLDP.parseONOSLLDP(eth);
        if (onoslldp != null) {
            Type lt;
            if (notMy(eth.getSourceMAC().toString())) {
                lt = Type.EDGE;
            } else {
                lt = eth.getEtherType() == Ethernet.TYPE_LLDP ?
                        Type.DIRECT : Type.INDIRECT;

                /* Verify MAC in LLDP packets */
                if (!ONOSLLDP.verify(onoslldp, context.lldpSecret(), context.maxDiscoveryDelay())) {
                    log.warn("LLDP Packet failed to validate, timestamp!");
                    return true;
                }
            }

            PortNumber srcPort = portNumber(onoslldp.getPort());
            PortNumber dstPort = packetContext.inPacket().receivedFrom().port();



            String idString = onoslldp.getDeviceString();
            if (!isNullOrEmpty(idString)) {
                try {
                    DeviceId srcDeviceId = DeviceId.deviceId(idString);
                    DeviceId dstDeviceId = packetContext.inPacket().receivedFrom().deviceId();

                    ConnectPoint src = new ConnectPoint(srcDeviceId, srcPort);
                    ConnectPoint dst = new ConnectPoint(dstDeviceId, dstPort);

                    LinkDescription ld = new DefaultLinkDescription(src, dst, lt);



                    
                    //Calculate the statistic
                    String strTmp = statisticsCal(srcDeviceId, srcPort, dstDeviceId, dstPort);
                    float plLink = 0;
                    float rLink = 0;
                    if (strTmp != ""){
                        String[] strPara = strTmp.split(",");
                        plLink = Float.parseFloat(strPara[0]);
                        rLink = Float.parseFloat(strPara[1]);
                    }else{
                        plLink = 0;
                        rLink = 0;
                    }

                    Timestamp current_timestamp_para = new Timestamp(System.currentTimeMillis());
                    String idPort = srcDeviceId.toString()+"-"+srcPort.toString();

                    //Calculate 
                    long delay = delayCal(onoslldp, idPort);
                    log.info("Link from {}:{} to {}:{}, Delay: {} ms", srcDeviceId, srcPort, dstDeviceId, dstPort, delay);
                    

                    //Calculate the breakpoint
                    breakPointCal(idPort, current_timestamp_para, delay, plLink, rLink);



                    context.providerService().linkDetected(ld);
                    context.touchLink(LinkKey.linkKey(src, dst));

                } catch (IllegalStateException | IllegalArgumentException e) {
                    log.warn("There is a exception during link creation: {}", e.getMessage());
                    return true;
                }
                return true;
            }
        }
        return false;
    }

    private boolean processLldp(PacketContext packetContext, Ethernet eth) {
        ONOSLLDP onoslldp = ONOSLLDP.parseLLDP(eth);
        if (onoslldp != null) {
            Type lt = eth.getEtherType() == Ethernet.TYPE_LLDP ?
                    Type.DIRECT : Type.INDIRECT;

            DeviceService deviceService = context.deviceService();
            MacAddress srcChassisId = onoslldp.getChassisIdByMac();
            String srcPortName = onoslldp.getPortNameString();
            String srcPortDesc = onoslldp.getPortDescString();

            log.debug("srcChassisId:{}, srcPortName:{}, srcPortDesc:{}", srcChassisId, srcPortName, srcPortDesc);

            if (srcChassisId == null && srcPortDesc == null) {
                log.warn("there are no valid port id");
                return false;
            }

            Optional<Device> srcDevice = findSourceDeviceByChassisId(deviceService, srcChassisId);

            if (!srcDevice.isPresent()) {
                log.warn("source device not found. srcChassisId value: {}", srcChassisId);
                return false;
            }
            Optional<Port> sourcePort = findSourcePortByName(
                    srcPortName == null ? srcPortDesc : srcPortName,
                    deviceService,
                    srcDevice.get());

            if (!sourcePort.isPresent()) {
                log.warn("source port not found. sourcePort value: {}", sourcePort);
                return false;
            }

            PortNumber srcPort = sourcePort.get().number();
            PortNumber dstPort = packetContext.inPacket().receivedFrom().port();

            DeviceId srcDeviceId = srcDevice.get().id();
            DeviceId dstDeviceId = packetContext.inPacket().receivedFrom().deviceId();

            if (!sourcePort.get().isEnabled()) {
                log.debug("Ports are disabled. Cannot create a link between {}/{} and {}/{}",
                        srcDeviceId, sourcePort.get(), dstDeviceId, dstPort);
                return false;
            }

            ConnectPoint src = new ConnectPoint(srcDeviceId, srcPort);
            ConnectPoint dst = new ConnectPoint(dstDeviceId, dstPort);

            DefaultAnnotations annotations = DefaultAnnotations.builder()
                    .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase())
                    .set(AnnotationKeys.LAYER, ETHERNET)
                    .build();

            LinkDescription ld = new DefaultLinkDescription(src, dst, lt, true, annotations);
            try {
                context.providerService().linkDetected(ld);
                context.setTtl(LinkKey.linkKey(src, dst), onoslldp.getTtlBySeconds());
            } catch (IllegalStateException e) {
                log.debug("There is a exception during link creation: {}", e);
                return true;
            }
            return true;
        }
        return false;
    }

    private Optional<Device> findSourceDeviceByChassisId(DeviceService deviceService, MacAddress srcChassisId) {
        Supplier<Stream<Device>> deviceStream = () ->
                StreamSupport.stream(deviceService.getAvailableDevices().spliterator(), false);
        Optional<Device> remoteDeviceOptional = deviceStream.get()
                .filter(device -> device.chassisId() != null
                        && MacAddress.valueOf(device.chassisId().value()).equals(srcChassisId))
                .findAny();

        if (remoteDeviceOptional.isPresent()) {
            log.debug("sourceDevice found by chassis id: {}", srcChassisId);
            return remoteDeviceOptional;
        } else {
            remoteDeviceOptional = deviceStream.get().filter(device ->
                    Tools.stream(deviceService.getPorts(device.id()))
                            .anyMatch(port -> port.annotations().keys().contains(AnnotationKeys.PORT_MAC)
                                    && MacAddress.valueOf(port.annotations().value(AnnotationKeys.PORT_MAC))
                                    .equals(srcChassisId)))
                    .findAny();
            if (remoteDeviceOptional.isPresent()) {
                log.debug("sourceDevice found by port mac: {}", srcChassisId);
                return remoteDeviceOptional;
            } else {
                return Optional.empty();
            }
        }
    }

    private Optional<Port> findSourcePortByName(String remotePortName,
                                                DeviceService deviceService,
                                                Device remoteDevice) {
        if (remotePortName == null) {
            return Optional.empty();
        }
        Optional<Port> remotePort = deviceService.getPorts(remoteDevice.id())
                .stream().filter(port -> Objects.equals(remotePortName,
                                                        port.annotations().value(AnnotationKeys.PORT_NAME)))
                .findAny();

        if (remotePort.isPresent()) {
            return remotePort;
        } else {
            return Optional.empty();
        }
    }

    // true if *NOT* this cluster's own probe.
    private boolean notMy(String mac) {
        // if we are using DEFAULT_MAC, clustering hadn't initialized, so conservative 'yes'
        String ourMac = context.fingerprint();
        if (ProbedLinkProvider.defaultMac().equalsIgnoreCase(ourMac)) {
            return true;
        }
        return !mac.equalsIgnoreCase(ourMac);
    }

    /**
     * Execute this method every t milliseconds. Loops over all ports
     * labeled as fast and sends out an LLDP. Send out an LLDP on a single slow
     * port.
     *
     * @param t timeout
     */
    @Override
    public void run(Timeout t) {
        if (isStopped()) {
            return;
        }

        if (context.mastershipService().isLocalMaster(deviceId)) {
            log.trace("Sending probes from {}", deviceId);
            ImmutableMap.copyOf(portMap).forEach(this::sendProbes);
        }

        if (!isStopped()) {
            timeout = t.timer().newTimeout(this, context.probeRate(), MILLISECONDS);
        }
    }

    /**
     * Creates packet_out LLDP for specified output port.
     *
     * @param portNumber the port
     * @param portDesc the port description
     * @return Packet_out message with LLDP data
     */
    private OutboundPacket createOutBoundLldp(Long portNumber, String portDesc) {
        if (portNumber == null) {
            return null;
        }
        ONOSLLDP lldp = getLinkProbe(portNumber, portDesc);
        if (lldp == null) {
            log.warn("Cannot get link probe with portNumber {} and portDesc {} for {} at LLDP packet creation.",
                    portNumber, portDesc, deviceId);
            return null;
        }
        ethPacket.setSourceMACAddress(context.fingerprint()).setPayload(lldp);
        return new DefaultOutboundPacket(deviceId,
                                         builder().setOutput(portNumber(portNumber)).build(),
                                         ByteBuffer.wrap(ethPacket.serialize()));
    }

    /**
     * Creates packet_out BDDP for specified output port.
     *
     * @param portNumber the port
     * @param portDesc the port description
     * @return Packet_out message with LLDP data
     */
    private OutboundPacket createOutBoundBddp(Long portNumber, String portDesc) {
        if (portNumber == null) {
            return null;
            
        }
        ONOSLLDP lldp = getLinkProbe(portNumber, portDesc);
        if (lldp == null) {
            log.warn("Cannot get link probe with portNumber {} and portDesc {} for {} at BDDP packet creation.",
                    portNumber, portDesc, deviceId);
            return null;
        }
        bddpEth.setSourceMACAddress(context.fingerprint()).setPayload(lldp);
        return new DefaultOutboundPacket(deviceId,
                                         builder().setOutput(portNumber(portNumber)).build(),
                                         ByteBuffer.wrap(bddpEth.serialize()));
    }

    private ONOSLLDP getLinkProbe(Long portNumber, String portDesc) {
        Device device = context.deviceService().getDevice(deviceId);
        if (device == null) {
            log.warn("Cannot find the device {}", deviceId);
            return null;
        }
        return ONOSLLDP.onosSecureLLDP(deviceId.toString(), device.chassisId(), portNumber.intValue(), portDesc,
                                       context.lldpSecret());
    }

    private void sendProbes(Long portNumber, String portDesc) {
        if (context.packetService() == null) {
            return;
        }
        log.trace("Sending probes out of {}@{}", portNumber, deviceId);
        //log.info("Sending probes out of {}@{}", portNumber, deviceId);
        OutboundPacket pkt = createOutBoundLldp(portNumber, portDesc);
        if (pkt != null) {
            context.packetService().emit(pkt);
        } else {
            log.warn("Cannot send lldp packet due to packet is null {}", deviceId);
        }
        if (context.useBddp()) {
            OutboundPacket bpkt = createOutBoundBddp(portNumber, portDesc);
            if (bpkt != null) {
                context.packetService().emit(bpkt);
            } else {
                log.warn("Cannot send bddp packet due to packet is null {}", deviceId);
            }
        }
    }

    public boolean containsPort(long portNumber) {
        return portMap.containsKey(portNumber);
    }
}
