### Name: Rclient
### Title: Functions to talk to an Rserve
### Aliases: RSconnect RSclose RSeval RSeval.old RSlogin RSdetach
###   RSevalDetach RSattach RSassign RSassign.old RSshutdown
### Keywords: interface

### ** Examples

## Not run: 
##D   c <- RSconnect()
##D   data(stackloss)
##D   RSassign(c, stackloss)
##D   RSeval(c, quote(library(MASS)))
##D   RSeval(c, quote(rlm(stack.loss ~ ., stackloss)$coeff))
##D   RSeval(c, "getwd()")
##D   
##D   image <- RSeval(c, quote(try({
##D     attach(stackloss)
##D     library(Cairo)
##D     Cairo(file="plot.png")
##D     plot(Air.Flow,stack.loss,col=2,pch=19,cex=2)
##D     dev.off()
##D     readBin("plot.png", "raw", 999999)})))
##D   if (inherits(image, "try-error"))
##D     stop(image)
## End(Not run)



