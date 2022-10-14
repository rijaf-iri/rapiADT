#' Plot wind frequencies contour.
#'
#' Get wind data to plot the direction frequencies with a contour plot and the speed with a boxplot.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

graphWindContours <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)

    saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    wind <- get_wind_data(user_req, aws_dir)
    if(wind$opts$status != 'plot') return(convJSON(wind))

    ######
    ttxt <- if(wind$opts$time_step == "hourly") "Hourly" else "Minutes"
    titre <- paste(ttxt, "wind data:", wind$opts$aws_name, "-", wind$opts$aws_id, "-", wind$opts$net_name)
    sub1 <- paste("Period:", wind$opts$start, "-", wind$opts$end)
    sub2 <- paste("Data availability,", "speed:", paste0(wind$data$ws_avail, "%"),
                  "-", "direction:", paste0(wind$data$wd_avail, "%"))
    subtitre <- paste0(sub1, '\n', sub2)

    ######
    dates <- as.POSIXct(wind$data$obs_time, origin = "1970-01-01", tz = wind$opts$tz)
    hr <- format(dates, "%H")

    width <- round(user_req$size[[1]])
    height <- round(user_req$size[[2]])
    pngfile <- tempfile()

    grDevices::png(pngfile, width = width, height = height)
    op <- graphics::par(mar = c(3.1, 3.1, 1, 1))

    windContours(hour = hr,
                 wd = wind$data$wd,
                 ws = wind$data$ws,
                 centre = user_req$centre,
                 ncuts = 0.5,
                 spacing = 2,
                 key.spacing = 2,
                 smooth.contours = 1.5,
                 smooth.fill = 1.5,
                 title = list(title = titre, subtitle = subtitre)
               )

    graphics::par(op)
    grDevices::dev.off()

    bin_data <- readBin(pngfile, "raw", file.info(pngfile)[1, "size"])
    bin_data <- RCurl::base64Encode(bin_data, "txt")
    png_base64 <- paste0("data:image/png;base64,", bin_data)


    # # #########
    # width <- round(user_req$size[[1]])
    # height <- round(user_req$size[[2]])
    # pngfile <- tempfile()

    # grDevices::png(pngfile, width = width, height = height)
    # op <- graphics::par(mar = c(3.1, 3.1, 1, 1))

    # plot(1:10, xlab = '', ylab = '', xaxt = 'n', yaxt = 'n')

    # graphics::par(op)
    # grDevices::dev.off()

    # bin_data <- readBin(pngfile, "raw", file.info(pngfile)[1, "size"])
    # bin_data <- RCurl::base64Encode(bin_data, "txt")
    # png_base64 <- paste0("data:image/png;base64,", bin_data)

    # # out <- list(opts = list(status = 'Test wind clim freq contour'), img = png_base64)
    # out <- list(opts = list(status = 'plot'), img = png_base64)

    # return(convJSON(out))
}


###########################

## adapted the function windContours from 
## https://github.com/tim-salabim/metvurst/

windContours <- function(hour, wd, ws, centre = "S",
                         ncuts = 0.5, spacing = 2, key.spacing = 2,
                         smooth.contours = 1.2, smooth.fill = 1.2,
                         colour = rev(RColorBrewer::brewer.pal(11, "Spectral")),
                         gapcolor = "grey50",
                         title = NULL)
{
    centre <- if(centre == "E") "ES" else centre
    levs <- switch(centre,
                   "N" = c(19:36, 1:18),
                   "ES" = c(28:36, 1:27),
                   "S" = 1:36,
                   "W" = c(10:36, 1:9))

    wdn <- c(45, 90, 135, 180, 225, 270, 315, 360)
    wdc <- c("NE", "E", "SE", "S", "SW", "W", "NW", "N")
    label <- switch(centre,
                    "N" = c(225, 270, 315, 360, 45, 90, 135, 180),
                    "ES" = c(315, 360, 45, 90, 135, 180, 225, 270),
                    "S" = c(45, 90, 135, 180, 225, 270, 315, 360),
                    "W" = c(135, 180, 225, 270, 315, 360, 45, 90))
    label <- wdc[match(label, wdn)]

    hour <- as.numeric(hour)
    dircat <- ordered(ceiling(wd/10), levels = levs, labels = 1:36)
    tab.wd <- stats::xtabs(~ dircat + hour)

    tab.wd_smooth <- fields::image.smooth(tab.wd, theta = smooth.contours, xwidth = 0, ywidth = 0)
    freq.wd <- matrix(prop.table(tab.wd_smooth$z, 2)[, 24:1] * 100, nrow = 36, ncol = 24)

    tab.add_smooth <- fields::image.smooth(tab.wd, theta = smooth.fill, xwidth = 0, ywidth = 0)
    mat.add <- matrix(prop.table(tab.add_smooth$z, 2)[, 24:1] * 100,  nrow = 36, ncol = 24)

    zlevs.fill <- seq(floor(min(mat.add)), ceiling(max(mat.add)), by = ncuts)
    zlevs.conts <- seq(floor(min(freq.wd)), ceiling(max(freq.wd)), by = spacing)

    kolorfun <- grDevices::colorRampPalette(colour)
    kolor <- kolorfun(length(zlevs.fill) - 1)

    mat.add <- rbind(mat.add, mat.add[1, ])
    freq.wd <- rbind(freq.wd, freq.wd[1, ])
    xax <- 1:37
    yax <- 0:23

    ####

    layout.matrix <- matrix(c(0, 1, 1, 1, 1, 0,
                              2, 2, 2, 3, 3, 3,
                              0, 0, 4, 4, 0, 0,
                              0, 0, 5, 5, 0, 0),
                              ncol = 6, nrow = 4, byrow = TRUE)
    layout(layout.matrix, widths = 1, heights = c(0.2, 0.8, 0.07, 0.07), respect = FALSE)

    ####

    op <- par(mar = c(0.1, 1, 1, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1.1, title$title, font = 2, cex = 2)
    text(1, 0.8, title$subtitle, font = 1, cex = 1.6)
    par(op)

    #### 
    op <- par(mar = c(5.1, 5.1, 0, 0))
    plot(1, xlim = range(xax), ylim = c(-0.5, 23.5), xlab = "", ylab = "",
         type = "n", xaxt = 'n', yaxt = 'n', xaxs = "i", yaxs = "i")
    axis(side = 1, at = seq(4.5, 36 ,by = 4.5), labels = label, font = 2, cex.axis = 1.2)
    axis(side = 2, at = seq(22, 2, -2) - 1, labels = seq(2, 22, 2), las = 1, font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Direction", font = 2, cex = 0.9)
    mtext(side = 2, line = 3, "Hour", font = 2, cex = 0.9)
    box()

    .filled.contour(xax, yax, mat.add, levels = zlevs.fill, col = kolor)
    .filled.contour(xax, yax, mat.add, levels = seq(0, 0.2, 0.1), col = gapcolor)
    contour(xax, yax, freq.wd, add = TRUE, levels = zlevs.conts, col = "grey10", labcex = 0.7,
            labels = seq(zlevs.fill[1], zlevs.fill[length(zlevs.fill)], key.spacing))
    contour(xax, yax, freq.wd, add = TRUE, levels = 0.5, col = "grey10", lty = 3, labcex = 0.7)
    par(op)

    ####
    op <- par(mar = c(5.1, 0, 0, 1.2))
    plot(1, xlim = range(ws, na.rm = TRUE) + c(-0.1, 0.5), ylim = c(0.5, 24.5), type = 'n',
         xaxt = 'n', yaxt = 'n', xlab = "", ylab = "", xaxs = "i", yaxs = "i")
    abline(h = seq(22, 2, -2), col = "lightgray", lty = "dotted", lwd = 1.0)
    abline(v = axTicks(1), col = "lightgray", lty = "solid", lwd = 0.9)
    axis(side = 1, at = axTicks(1), font = 2, cex.axis = 1.2)
    mtext(side = 1, line = 3, "Speed [m/s]", font = 2, cex = 0.9)

    boxplot(ws ~ rev(hour), horizontal = TRUE, xaxt = 'n', yaxt = 'n', add = TRUE, notch = TRUE,
            col = 'lightblue', medcol = 'red', whiskcol = 'blue', staplecol = 'blue',
            boxcol = 'blue', outcol = 'blue', outbg = 'lightblue', outcex = 0.7, outpch = 21, boxwex = 0.4)
    par(op)

    ####
    op <- par(mar = c(2.0, 1, 0, 1))
    nBreaks<- length(zlevs.fill)
    midpoints<- (zlevs.fill[1:(nBreaks - 1)] +  zlevs.fill[2:nBreaks])/2
    mat.lez <- matrix(midpoints, nrow = 1, ncol = length(midpoints)) 
    image(zlevs.fill, 1:2, t(mat.lez), col = kolor, breaks = zlevs.fill, xaxt = "n", yaxt = "n", xlab = "", ylab = "")
    axis.args <- list(side = 1, mgp = c(3, 1, 0), las = 0, font = 2, cex.axis = 1.5)
    do.call("axis", axis.args)
    box()
    par(op)

    op <- par(mar = c(0, 1, 0, 1))
    plot(1, type = 'n', xaxt = 'n', yaxt = 'n', xlab = '', ylab = '', bty = "n")
    text(1, 1, "Frequencies (in %)", font = 2, cex = 1.5)
    par(op)

    invisible()
}


download_wind_contours <- function(hour, wd, ws){
    hour <- as.numeric(hour)
    dircat <- ordered(ceiling(wd/10), levels = 1:36, labels = 1:36)
    tab.wd <- stats::xtabs(~ dircat + hour)

    h.wd <- as.integer(colnames(tab.wd))
    mat.wd <- matrix(NA, 36, 24)
    mat.wd[, h.wd + 1] <- tab.wd
    lab.wd <- paste0(']', seq(0, 350, 10), ' - ', seq(10, 360, 10), ']')

    mat.ws <- lapply(0:23, function(h){
        s <- ws[hour == h]
        n0 <- length(s)
        s <- s[!is.na(s)]
        n1 <- length(s)
        if(n1 == 0){
            sm <- rep(NA, 6)
        }else{
            sm <- summary(s)
            sm <- round(as.numeric(sm[1:6]), 6)
        }
        c(sm, n0 - n1, n0)
    })
    mat.ws <- do.call(cbind, mat.ws)

    lab.ws <- c('Minimum', '1st Quartile', 'Median', 'Mean',
                '3rd Quartile', 'Maximum', 'Number Missing Values',
                'Number of Observations')

    o_wd <- cbind(lab.wd, mat.wd)
    o_ws <- cbind(lab.ws, mat.ws)

    out <- rbind(c('Wind direction/Hour', 0:23), o_wd,
                  rep('*-*', 25),
                 c('Wind speed statistics/Hour', 0:23), o_ws)

    convCSV(out, col.names = FALSE)
}
