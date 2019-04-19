library(breakpoint)
simdata <- as.data.frame(c(rnorm(200,100,5),rnorm(100,300,5),rnorm(300,150,5)))

## CE with four parameter beta distribution with mBIC as the selection criterion  ##
obj1 <- CE.Normal.Init.Mean(simdata, init.locs = c(150, 380), distyp = 1, parallel =TRUE)
## Print the location
obj1$$BP.Loc
