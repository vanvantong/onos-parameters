# Network parameters in ONOS
- Calculate some network parameters of links (delay, packet loss and link utilization on each link)
- Change above files following below directories to measure parmameters against time (please backup these files before changing it).
- These files are used for ONOS 2.4.
- Directories:
LinkDiscovery: onos/providers/lldpcommon/src/main/java/org/onosproject/provider/lldpcommon/LinkDiscovery.java

ONOSLLDP: onos/utils/misc/src/main/java/org/onlab/packet/ONOSLLDP.java

LldpLinkProvider: onos/providers/lldp/src/main/java/org/onosproject/provider/lldp/impl/LldpLinkProvider.java

