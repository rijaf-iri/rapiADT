image.plot_Legend_pars <- function(Zmat, userOp){
    brks0 <- pretty(Zmat, n = 10, min.n = 5)
    brks0 <- if(length(brks0) > 0) brks0 else c(0, 1)
    breaks <- if(userOp$levels$custom) userOp$levels$levels else brks0
    breaks[length(breaks)] <- breaks[length(breaks)] + 1e-15

    ## legend label breaks
    legend.label <- breaks

    breaks1 <- if(userOp$levels$equidist) seq(0, 1, length.out = length(breaks)) else breaks

    Zmat <- Zmat + 1e-15
    Zrange <- if(all(is.na(Zmat))) c(0, 1) else range(Zmat, na.rm = TRUE)

    brks0 <- range(brks0)
    brks1 <- range(breaks)
    brn0 <- min(brks0[1], brks1[1])
    brn1 <- max(brks0[2], brks1[2])
    if(brn0 == breaks[1]) brn0 <- brn0 - 1
    if(brn1 == breaks[length(breaks)]) brn1 <- brn1 + 1
    if(brn0 > Zrange[1]) brn0 <- Zrange[1]
    if(brn1 < Zrange[2]) brn1 <- Zrange[2]
    breaks <- c(brn0, breaks, brn1)
    tbrks2 <- breaks1[c(1, length(breaks1))] + diff(range(breaks1)) * 0.02 * c(-1, 1)
    breaks2 <- c(tbrks2[1], breaks1, tbrks2[2])
    zlim <- range(breaks2)

    ## colors
    if(userOp$uColors$custom){
        kolFonction <- grDevices::colorRampPalette(userOp$uColors$color)
        kolor <- kolFonction(length(breaks) - 1)
    }else{
        kolFonction <- get(userOp$pColors$color, mode = "function")
        kolor <- kolFonction(length(breaks) - 1)
        if(userOp$pColors$reverse) kolor <- rev(kolor)
    }
    
    ## bin
    ## < x1; [x1, x2[; [x2, x3[; ...; [xn-1, xn]; > xn

    list(breaks = breaks,
         legend.breaks = list(zlim = zlim, breaks = breaks2),
         legend.axis = list(at = breaks1, labels = legend.label),
         colors = kolor)
}

#####
