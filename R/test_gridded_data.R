
#' Plot gridded data, PNG.
#'
#' Format gridded data to display on map.
#'
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

griddedPNGBase64 <- function(aws_dir){
    plotType <- 'pixel'
    # plotType <- 'pixel'
    smoothImage <- FALSE #TRUE
    # 1:10 number of grid right/top/left/bottom (2 * smoothGrid + 1)
    smoothGrid <- 1
    # 0-->2 (take resGrid * n, n = 1:4)
    smoothSpan <- 0.1

    ################
    ## nc data
    ncfile <- file.path(aws_dir, "AWS_DATA", "DATA", "gridded_test", "chirps_202203.nc")
    nc <- ncdf4::nc_open(ncfile)
    lon <- nc$dim[[1]]$vals
    lat <- nc$dim[[2]]$vals
    mat <- ncdf4::ncvar_get(nc, nc$var[[1]]$name)
    ncdf4::nc_close(nc)

    ## levels
    # brks <- c(1, 10, 20, 40, 60, 80, 100, 130, 160, 190, 210)
    brks <- pretty(mat, n = 10)
    ## colors
    kols <- fields::tim.colors(20)

    # ################
    # nc <- gridded_circle()
    # lon <- nc$x
    # lat <- nc$y
    # mat <- nc$z

    # ## breaks
    # brks <- seq(10, 60, 10)
    # ## colors
    # kols <- fields::tim.colors(20)

    ################

    if(smoothImage){
        # neighbourhood smoothing, moving average 
        # mat <- smooth.matrix(mat, smoothGrid)
        # neighbourhood smoothing, loess, polynomial surface degree 2
        sm <- loess.smoothing.matrix(list(x = lon, y = lat, z = mat), smoothSpan)
        mat <- sm$z
    }

    ################

    ## colorkey
    userOp <- list(uColors = list(custom = TRUE, color = kols),
                   levels = list(custom = TRUE, levels = brks, equidist = TRUE))
    ckey <- image.plot_Legend_pars(mat, userOp)
    ckeys <- list(labels = ckey$legend.axis$labels, colors = ckey$colors,
                  title = "Precipitation (mm)")

    ##
    nx <- length(lon)
    ny <- length(lat)
    width <- 2 * nx
    height <- 2 * ny
    rx0 <- (lon[2] - lon[1])/2
    rx1 <- (lon[nx] - lon[nx - 1])/2
    bndx <- range(lon) + c(-rx0, rx1)
    ry0 <- (lat[2] - lat[1])/2
    ry1 <- (lat[ny] - lat[ny - 1])/2
    bndy <- range(lat) + c(-ry0, ry1)
    bounds <- cbind(rev(bndy), bndx)

    ###
    pngfile <- tempfile()

    grDevices::png(pngfile, width = width, height = height)
    op <- graphics::par(mar = c(0, 0, 0, 0))

    if(plotType == 'pixel'){
        graphics::image(mat, breaks = ckey$breaks, col = ckey$colors,
                        xaxt = 'n', yaxt = 'n', bty = 'n')
    }else{
        plot(1, xlim = range(lon), ylim = range(lat), xlab = "", ylab = "",
            type = "n", xaxt = 'n', yaxt = 'n', bty = 'n')
        graphics::.filled.contour(lon, lat, mat, levels = ckey$breaks, col = ckey$colors)
    }

    graphics::par(op)
    grDevices::dev.off()

    bin_data <- readBin(pngfile, "raw", file.info(pngfile)[1, "size"])
    bin_data <- RCurl::base64Encode(bin_data, "txt")
    png_base64 <- paste0("data:image/png;base64,", bin_data)

    ###### 
    tmp = matrix(ckey$breaks[-1], ncol = 1)
    grDevices::png(pngfile, width = length(ckey$colors), height = 1)
    op <- graphics::par(mar = c(0, 0, 0, 0))
    graphics::image(tmp, breaks = ckey$breaks, col = ckey$colors,
                    xaxt = 'n', yaxt = 'n', bty = 'n')
    graphics::par(op)
    grDevices::dev.off()

    ckey_data <- readBin(pngfile, "raw", file.info(pngfile)[1, "size"])
    ckey_data <- RCurl::base64Encode(ckey_data, "txt")
    ckeys$png <- paste0("data:image/png;base64,", ckey_data)

    unlink(pngfile)

    out <- list(status = "ok", message = NULL, flash = 'error',
                date = "2022-03", ckeys = ckeys,
                data = list(png = png_base64, bounds = bounds),
                title = "Monthly rainfall CHIRPS")

    return(convJSON(out))
}

############################

#' Plot gridded data, Contour.
#'
#' Format gridded data to display on map.
#'
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

griddedCountorMap <- function(aws_dir){

    ############################
    ### Data send for Contour Map using d3-contour

    # out <- get_data_for_d3_contour(aws_dir)

    ############################
    ### Contour Map send as geojson polygons

    out <- create_contour_polys(aws_dir)
    
    return(convJSON(out))
}

############################

get_data_for_d3_contour <- function(aws_dir){
    ## nc data
    ncfile <- file.path(aws_dir, "AWS_DATA", "DATA", "gridded_test", "chirps_tmax_20161225.nc")
    nc <- ncdf4::nc_open(ncfile)
    lon <- nc$dim[[1]]$vals
    lat <- nc$dim[[2]]$vals
    mat <- ncdf4::ncvar_get(nc, nc$var[[1]]$name)
    ncdf4::nc_close(nc)

    ########

    mat <- smooth.matrix(mat, 3)

    # ## levels
    # brks <- pretty(mat, n = 10)
    brks <- seq(20, 30, 2)

    ## colors
    kols <- fields::tim.colors(20)

    ## colorkey
    userOp <- list(uColors = list(custom = TRUE, color = kols),
                   levels = list(custom = TRUE, levels = brks, equidist = TRUE))
    ckey <- image.plot_Legend_pars(mat, userOp)
    ckeys <- list(labels = ckey$legend.axis$labels, colors = ckey$colors,
                  title = "Maximum Temperature (C)")

    # mat need to transpose
    data <- list(x = lon, y = lat, z = t(mat))
    out <- list(status = "ok", message = NULL, flash = 'error',
                date = "2022-03", ckeys = ckeys, data = data,
                title = "Test contour map"
            )

    return(out)
}

############################

create_contour_polys <- function(aws_dir){
    ## nc data
    ncfile <- file.path(aws_dir, "AWS_DATA", "DATA", "gridded_test", "chirps_tmax_20161225.nc")
    nc <- ncdf4::nc_open(ncfile)
    lon <- nc$dim[[1]]$vals
    lat <- nc$dim[[2]]$vals
    mat <- ncdf4::ncvar_get(nc, nc$var[[1]]$name)
    ncdf4::nc_close(nc)

    mat <- smooth.matrix(mat, 3)

    # ## levels
    # brks <- pretty(mat, n = 10)
    brks <- seq(20, 30, 2)

    ## colors
    kols <- fields::tim.colors(20)

    ## colorkey
    userOp <- list(uColors = list(custom = TRUE, color = kols),
                   levels = list(custom = TRUE, levels = brks, equidist = TRUE))
    ckey <- image.plot_Legend_pars(mat, userOp)
    ckeys <- list(labels = ckey$legend.axis$labels, colors = ckey$colors,
                  title = "Maximum Temperature (C)")

    # contour(lon, lat, mat, levels = ckey$breaks, col = ckey$colors)
    # 
    # plot(1, xlim = range(lon), ylim = range(lat), xlab = "", ylab = "", type = "n", xaxt = 'n', yaxt = 'n', bty = 'n')
    # graphics::.filled.contour(lon, lat, mat, levels = ckey$breaks, col = ckey$colors)
    #
    # graphics::image(mat, breaks = ckey$breaks, col = ckey$colors, xaxt = 'n', yaxt = 'n', bty = 'n')
    # 
    # fields::image.plot(lon, lat, mat, breaks = ckey$breaks, col = ckey$colors)

    ## create contour plot
    cont <- grDevices::contourLines(lon , lat , mat, levels = ckey$breaks)

    # # ## plot(1, xlim = range(lon), ylim = range(lat), xaxs = 'i', yaxs = 'i')
    # plot(1, xlim = range(lon), ylim = range(lat))
    # for(j in seq_along(cont)){
    #     lines(cont[[j]]$x, cont[[j]]$y)
    # }
    # p <- cont[[2]]
    # n <- length(p$x)
    # lines(p$x, p$y, col = 2)
    # segments(p$x[1], p$y[1], p$x[n], p$y[n], col = '4')


    # pos <- lapply(cont, function(p){
    #     n <- length(p$x)
    #     if(p$x[1] == p$x[n] && p$y[1] == p$y[n]){
    #         return(c('ok', 'ok', 'ok', 'ok'))
    #     }

    #     if(p$x[1] == p$x[n] || p$y[1] == p$y[n]){
    #         pos <- c('ok', 'ok', 'ok', 'ok')
    #     }else{
    #         pos <- line_start_end_pos(p, xlim, ylim)
    #     }
    #     return(pos)
    # })


    # plot(1, xlim = range(lon), ylim = range(lat))
    # for(j in seq_along(cont)){
    #     n <- length(cont[[j]]$x)
    #     lines(cont[[j]]$x, cont[[j]]$y)
    #     text(cont[[j]]$x[1], cont[[j]]$y[1], paste0('s-', j))
    #     text(cont[[j]]$x[n], cont[[j]]$y[n], paste0('e-', j))
    # }

    xlim <- range(lon)
    ylim <- range(lat)
    corner <- matrix(c(c(xlim[1], ylim[1]), c(xlim[1], ylim[2]),
                       c(xlim[2], ylim[2]), c(xlim[2], ylim[1])),
                     ncol = 2, byrow = TRUE)

    cont <- lapply(cont, function(p){
        n <- length(p$x)
        if(p$x[1] == p$x[n] && p$y[1] == p$y[n]){
            return(p)
        }
        if(p$x[1] == p$x[n] || p$y[1] == p$y[n]){
            p$x <- c(p$x, p$x[1])
            p$y <- c(p$y, p$y[1])
        }else{
            pos <- line_start_end_pos(p, xlim, ylim)

            if(pos[1] == 'm' && pos[3] == 'm'){
                p$x <- c(p$x, xlim[1], xlim[1], p$x[1])
                if(pos[2] == 'b'){
                    p$y <- c(p$y, ylim[2], ylim[1], p$y[1])
                }else{
                    p$y <- c(p$y, ylim[1], ylim[2], p$y[1])
                }
            }else if(pos[2] == 'm' && pos[4] == 'm'){
                p$y <- c(p$y, ylim[1], ylim[1], p$y[1])
                if(pos[1] == 'r'){
                    p$x <- c(p$x, xlim[1], xlim[2], p$x[1])
                }else{
                    p$x <- c(p$x, xlim[2], xlim[1], p$x[1])
                }
            }else{
                m <- c((p$x[1] + p$x[n])/2, c(p$y[1] + p$y[n])/2)
                d <- sqrt((m[1] - corner[, 1])^2 + (m[2] - corner[, 2])^2)
                mp <- corner[which.min(d), ]
                p$x <- c(p$x, mp[1], p$x[1])
                p$y <- c(p$y, mp[2], p$y[1])
            }
        }

        return(p)
    })

    corner <- rbind(corner, corner[1, ])
    pl <- list(level = NA, x = corner[, 1], y = corner[, 2])
    cont[[length(cont) + 1]] <- pl

    # plot(1, xlim = range(lon), ylim = range(lat))
    # kol <- rainbow(length(cont))
    # for(j in seq_along(cont1)){
    #     lines(cont[[j]]$x, cont[[j]]$y, col = kol[j])
    # }

    polys <- lapply(seq_along(cont), function(i){
        pl <- sp::Polygon(cbind(cont[[i]]$x, cont[[i]]$y))
        sp::Polygons(list(pl), ID = paste0('p', i))
    })

    polys <-  sp::SpatialPolygons(polys, seq_along(polys))

    # sp::plot(polys)

    polys <- sf::st_as_sf(polys)
    # plot(polys)
    polys <- sf::st_intersection(polys)
    polys <- methods::as(polys, "Spatial")
    polys <- methods::as(polys, "SpatialPolygons")

    dat_sp <- expand.grid(x = lon, y =  lat)
    sp::coordinates(dat_sp) <- c('x', 'y')
    dat_sp <- sp::SpatialPixels(points = dat_sp, tolerance = 0.0002,
                            proj4string = sp::CRS(as.character(NA)))

    dat_info <- lapply(seq_along(polys), function(j){
        # sp::bbox(polys[j])
        ix <- sp::over(dat_sp, polys[j])
        # sp::plot(dat_sp[!is.na(ix)])
        x <- mat[!is.na(ix)]
        # c(range(x, na.rm = TRUE), mean(x, na.rm = TRUE))
        m <- mean(x, na.rm = TRUE)
        if(is.nan(m)){
            col <- "#FFFFFF"
            int <- NA
        }else{
            ip <- findInterval(m, ckeys$labels, rightmost.closed = TRUE, left.open = TRUE)
            col <- ckeys$colors[ip + 1]
            if(ip == 0){
                int <- paste('<', ckeys$labels[1])
            }else if(ip == length(ckeys$labels)){
                int <- paste('>', ckeys$labels[length(ckeys$labels)])
            }else{
                int <- paste('[', ckeys$labels[ip], '-', ckeys$labels[ip + 1], ']')
            }
       }

       list(color = col, values = int)
    })

    dat_info <- do.call(rbind.data.frame, dat_info)
    rownames(dat_info) <- sapply(methods::slot(polys, "polygons"),
                                 function(x) methods::slot(x, "ID"))
    polys_df <- sp::SpatialPolygonsDataFrame(polys, dat_info)

    tmpf <- '~/Desktop/test.geojson'
    tmp <- geojsonio::geojson_json(polys_df)
    geojsonio::geojson_write(tmp, file = tmpf)
    tmp <- readLines(tmpf, warn = FALSE)
    unlink(tmpf)

    out <- list(status = "ok", message = NULL, flash = 'error',
                date = "2022-03", ckeys = ckeys, data = tmp,
                title = "Test contour map")

    return(out)
}


line_start_end_pos <- function(p, xlim, ylim){
    n <- length(p$x)
    if(p$x[1] == xlim[1]){
        xs <- 'l'
    }else if(p$x[1] == xlim[2]){
        xs <- 'r'
    }else{
        xs <- 'm'
    }

    if(p$x[n] == xlim[1]){
        xe <- 'l'
    }else if(p$x[n] == xlim[2]){
        xe <- 'r'
    }else{
        xe <- 'm'
    }

    if(p$y[1] == ylim[1]){
        ys <- 'b'
    }else if(p$y[1] == ylim[2]){
        ys <- 't'
    }else{
        ys <- 'm'
    }

    if(p$y[n] == ylim[1]){
        ye <- 'b'
    }else if(p$y[n] == ylim[2]){
        ye <- 't'
    }else{
        ye <- 'm'
    }

    c(xs, ys, xe, ye)
}

##########################

# x <- list(x = lon, y = lat, z = mat)
loess.smoothing.matrix <- function(x, span = 0.1){
    xy <- expand.grid(x[c('x', 'y')])
    dat <- cbind(xy, z = c(x$z))
    fun <- stats::loess(z ~ x+y, data = dat, span = span,
                        degree = 2, na.action = stats::na.omit)
    sdat <- stats::predict(fun, newdata = xy)
    sdat <- array(sdat, sapply(x[c('x', 'y')], length))
    x$z <- sdat

    return(x)
}

# x <- data.frame(x = lon, y = lat, z = obs)
# grid <- list(x = x_grid, y = y_grid)
loess.smoothing.points <- function(x, grid, span = 0.1){
    xy <- expand.grid(grid)
    fun <- stats::loess(z ~ x+y, data = x, span = span,
                        degree = 2, na.action = stats::na.omit)
    sdat <- stats::predict(fun, newdata = xy)
    sdat <- array(sdat, sapply(grid, length))

    return(c(grid, list(z = sdat)))
}



##########################

smooth.matrix <- function(mat, ns){
    res <- cdt.matrix.mw(mat, ns, ns, mean, na.rm = TRUE)
    res[is.nan(res)] <- NA
    return(res)
}

# Calculate moving window values for the neighborhood of a center grid
cdt.matrix.mw <- function(x, sr, sc, fun, ...){
    fun <- match.fun(fun)
    nr <- nrow(x)
    nc <- ncol(x)
    res <- x * NA
    for(j in 1:nc){
        for(i in 1:nr){
            ir <- i + (-sr:sr)
            ir <- ir[ir > 0 & ir <= nr]
            ic <- j + (-sc:sc)
            ic <- ic[ic > 0 & ic <= nc]
            res[i, j] <- fun(c(x[ir, ic]), ...)
        }
    }
    # res[is.nan(res) | is.infinite(res)] <- NA
    return(res)
}

cdt.2matrices.mv <- function(x, y, sr, sc, fun, ...){
    stopifnot(dim(x) == dim(y))
    fun <- match.fun(fun)
    nr <- nrow(x)
    nc <- ncol(x)
    res <- x * NA
    for(j in 1:nc){
        for(i in 1:nr){
            ir <- i + (-sr:sr)
            ir <- ir[ir > 0 & ir <= nr]
            ic <- j + (-sc:sc)
            ic <- ic[ic > 0 & ic <= nc]
            res[i, j] <- fun(c(x[ir, ic]), c(y[ir, ic]), ...)
        }
    }
    # res[is.nan(res) | is.infinite(res)] <- NA
    return(res)
}


# ################################

layoutHeightsCollapse <- function(...) {
  x.settings <- lattice::trellis.par.get()$layout.heights
  x.settings[] <- 0
  x.settings$panel <- 1
  inputs <- list(...)
  if (length(inputs))
    x.settings[names(inputs)] <- inputs
  x.settings
}

layoutWidthsCollapse <- function(...) {
  y.settings <- lattice::trellis.par.get()$layout.widths
  y.settings[] <- 0
  y.settings$panel <- 1
  inputs <- list(...)
  if (length(inputs))
    y.settings[names(inputs)] <- inputs
  y.settings
}

axisComponentsCollapse <- function(...) {
  ax.settings <- lattice::trellis.par.get()$axis.components
  ax.settings$top[] <- 0
  ax.settings$right[] <- 0
  ax.settings$bottom[] <- 0
  ax.settings$left[] <- 0
  inputs <- list(...)
  if (length(inputs))
    ax.settings[names(inputs)] <- inputs
  ax.settings
}


############################

gridded_circle <- function(){
    center <- list(x = 30, y = -2)
    x_grd <- seq(29, 31, 0.01)
    y_grd <- seq(-3, -1, 0.01)
    val_grd <- seq(5, 60, 5)

    radius <- seq(1/12, 1, 1/12)
    theta <- seq(0, 2 * pi, length = 200)

    polys <- lapply(radius, function(r){
        xc <- center$x + r * cos(theta)
        yc <- center$y + r * sin(theta)

        poly <- sp::Polygon(cbind(xc, yc))
        poly1 <- sp::Polygons(list(poly), ID = "id1")
        sp::SpatialPolygons(list(poly1))
    })

    dat_grd <- matrix(NA, length(x_grd), length(y_grd))
    grd <- expand.grid(x = x_grd, y = y_grd)
    sp::coordinates(grd) <- ~x+y
    grd <- sp::SpatialPixels(points = grd, tolerance = 0.0002, 
                proj4string = sp::CRS(as.character(NA)))

    for(j in seq_along(polys)){
        ij <- sp::over(grd, polys[[j]])
        ij <- as.integer(names(ij[!is.na(ij)]))
        ina <- which(is.na(dat_grd))
        ix <- intersect(ina, ij)
        dat_grd[ix] <- val_grd[j]
    }

    dat_grd[is.na(dat_grd)] <- 70

    list(x = x_grd, y = y_grd, z = dat_grd)
}

