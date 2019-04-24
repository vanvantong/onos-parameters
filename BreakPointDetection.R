library(breakpoint)
## Read data from file
network_feature <- read.csv(file = '/home/vantong/onos/providers/lldpcommon/src/main/java/org/onosproject/provider/lldpcommon/data.csv', header = FALSE, sep = ",")
## Delay
delay <- as.data.frame(t(network_feature[1,]))
## CE with four parameter beta distribution with mBIC as the selection criterion  ##
obj_delay <- CE.Normal.Init.Mean(delay, init.locs = c(3,4), distyp = 1, parallel =TRUE)
## Print the number of breakpoint in delay
obj_delay$No.BPs


## Packetloss
packetloss <- as.data.frame(t(network_feature[2,]))
## CE with four parameter beta distribution with mBIC as the selection criterion  ##
obj_packetloss <- CE.Normal.Init.Mean(packetloss, init.locs = c(3,4), distyp = 1, parallel =TRUE)
## Print the number of breakpoint in delay
obj_packetloss$No.BPs


## Rate
rate <- as.data.frame(t(network_feature[3,]))
## CE with four parameter beta distribution with mBIC as the selection criterion  ##
obj_rate <- CE.Normal.Init.Mean(rate, init.locs = c(3,4), distyp = 1, parallel =TRUE)
## Print the number of breakpoint in delay
obj_rate$No.BPs
