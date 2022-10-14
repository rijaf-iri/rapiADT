
#' Read AWS metadata for mod_auth module.
#'
#' Read AWS coordinates and parameters.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readCoordsPars <- function(aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, "aws_parameters_minutes.json")
    awsPars <- jsonlite::read_json(parsFile)
    colFile <- file.path(dirJSON, "coords_header_infos.json")
    colInfos <- jsonlite::read_json(colFile)

    crds <- readCoordsDB(aws_dir)

    id <- sapply(awsPars, '[[', 'id')
    ix <- match(id, crds$id)

    awsPars <- lapply(seq_along(id), function(j){
        x <- awsPars[[j]]
        ii <- ix[j]
        if(is.na(ii)) return(NULL)
        y <- as.list(crds[ix[j], ])
        nx <- names(x)
        nx <- nx[nx %in% names(y)]
        for(n in nx) x[[n]] <- y[[n]]
        x$startdate <- y$startdate
        x$enddate <- y$enddate

        return(x)
    })
    inull <- sapply(awsPars, is.null)
    awsPars <- awsPars[!inull]
    out <- list(data = awsPars, colinfo = colInfos)

    return(convJSON(out))
}

#' Read AWS metadata for mod_aws module.
#'
#' Read AWS coordinates and parameters for minutes, hourly and daily data.
#' 
#' @param time_step time step of the data: "minutes", "hourly" or "daily".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readAWSMetadata <- function(time_step, aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    colFile <- file.path(dirJSON, "coords_header_infos.json")
    colInfos <- jsonlite::read_json(colFile)

    awsPars <- formatAWSMetadata(time_step, aws_dir)

    out <- list(data = awsPars, colinfo = colInfos)

    return(convJSON(out))
}

#' Read AWS parameters metadata for mod_aws module.
#'
#' Read AWS coordinates and parameters for hourly and daily data.
#' 
#' @param time_step time step of the data: "minutes", "hourly" or "daily".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readVARSMetadata <- function(time_step, aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    colFile <- file.path(dirJSON, "coords_header_infos.json")
    colInfos <- jsonlite::read_json(colFile)

    awsPars <- formatAWSMetadata(time_step, aws_dir)

    v_order <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7)
    stats <- lapply(awsPars, '[[', 'stats')
    stats <- do.call(rbind, stats)
    stats <- stats[!duplicated(stats), , drop = FALSE]
    iv <- orderedVariables(stats$var_code, v_order)
    stats <- stats[iv, , drop = FALSE]
    rownames(stats) <- NULL

    cpars <- paste(stats$var_code, stats$height,
                   stats$stat_code, sep = '_')
    aws_stats <- lapply(awsPars, function(x){
        vprs <- paste(x$stats$var_code, x$stats$height,
                      x$stats$stat_code, sep = '_')
        iv <- match(cpars, vprs)
        ifelse(is.na(iv), FALSE, TRUE)
    })
    aws_stats <- do.call(rbind, aws_stats)
    colnames(aws_stats) <- cpars

    coords <- lapply(awsPars, '[[', 'coords')
    coords <- do.call(rbind, coords)
    coords <- cbind(coords, aws_stats)

    vars <- lapply(awsPars, '[[', 'params')
    vars <- do.call(rbind, vars)
    vars <- vars[!duplicated(vars), , drop = FALSE]

    vh <- paste0(stats$var_code, '_', stats$height)
    ivh <- split(seq(nrow(stats)), vh)
    params <- lapply(ivh, function(ih){
        x <- stats[ih, , drop = FALSE]
        v <- vars[vars$code == x$var_code[1], , drop = FALSE]
        v$height <- x$height[1]
        s <- x[, c('stat_code', 'stat_name'), drop = FALSE]
        v$stats <- list(s)
        v
    })
    params <- do.call(rbind, params)
    iv <- orderedVariables(params$code, v_order)
    params <- params[iv, , drop = FALSE]

    out <- list(params = params, coords = coords, colinfo = colInfos)

    return(convJSON(out))
}


formatAWSMetadata <- function(time_step, aws_dir){
    if(time_step %in% c('pentad', 'dekadal', 'monthly')){
        time_step <- 'daily'
    }
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
    awsPars <- readRDS(parsFile)
    net_aws <- sapply(awsPars, function(x){
        paste0(x$coords$network_code, '_', x$coords$id)
    })
    names(awsPars) <- net_aws

    crds <- readCoordsDB(aws_dir)
    crd_net_aws <- paste0(crds$network_code, '_', crds$id)
    ix <- match(net_aws, crd_net_aws)
    crds <- crds[ix, c('startdate', 'enddate'), drop = FALSE]
    for(j in seq(nrow(crds))){
       awsPars[[j]]$coords$startdate <- crds$startdate[j]
       awsPars[[j]]$coords$enddate <- crds$enddate[j]
    }

    v_order <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7)
    awsPars <- lapply(awsPars, function(x){
        v1 <- orderedVariables(x$params$code, v_order)
        x$params <- x$params[v1, , drop = FALSE]

        # precip sum only
        rr_sum <- !(x$stats$var_code == 5 & x$stats$stat_code %in% 1:3)
        x$stats <- x$stats[rr_sum, , drop = FALSE]

        v2 <- orderedVariables(x$stats$var_code, v_order)
        x$stats <- x$stats[v2, , drop = FALSE]
        return(x)
    })

    if(time_step == 'daily'){
        # 9, 10, 8, 14, 7, 15, 18 avg only
        # sum, avg, min, max only for others var
        var_choices <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7, 15, 18)
        awsPars <- lapply(awsPars, function(x){
            x$params <- x$params[x$params$code %in% var_choices, , drop = FALSE]
            x$stats <- x$stats[x$stats$var_code %in% var_choices, , drop = FALSE]
            ix <- x$stats$var_code %in% c(8, 10, 9, 14, 7, 15, 18)
            x1 <- x$stats[!ix, , drop = FALSE]
            x1 <- x1[x1$stat_code %in% 1:4, , drop = FALSE]
            x2 <- x$stats[ix, , drop = FALSE]
            x2 <- x2[x2$stat_code == 1, , drop = FALSE]
            x$stats <- rbind(x1, x2)
            return(x)
        })
    }

    return(awsPars)
}

##############################

#' Get AWS coordinates.
#'
#' Get AWS coordinates to display on map.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readCoordsMap <- function(aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    netFile <- file.path(dirJSON, "network_infos.json")
    netInfos <- jsonlite::read_json(netFile)
    net_seq <- names(netInfos)
    netKOLS <- unname(sapply(netInfos, "[[", "color"))
    colFile <- file.path(dirJSON, "coords_header_infos.json")
    colInfos <- jsonlite::read_json(colFile)

    #############

    crds <- readCoordsDB(aws_dir)

    xcrd <- crds[, c('longitude', 'latitude')]
    xcrd <- paste(xcrd[, 1], xcrd[, 2], sep = "_")
    ix1 <- duplicated(xcrd) & !is.na(crds$longitude)
    ix2 <- duplicated(xcrd, fromLast = TRUE) & !is.na(crds$longitude)
    ix <- ix1 | ix2
    icrd <- unique(xcrd[ix])

    #############

    crds <- apply(crds, 2, as.character)
    crds <- cbind(crds, StatusX = "blue")

    for(j in seq_along(netInfos))
        crds[crds[, "network"] == netInfos[[net_seq[j]]]$name, "StatusX"] <- netKOLS[j]

    #############
    if(length(icrd) > 0){
        for(jj in icrd){
            ic <- xcrd == jj
            xx <- apply(crds[ic, ], 2, paste0, collapse = " | ")
            xx <- matrix(xx, nrow = 1, dimnames = list(NULL, names(xx)))
            xx <- do.call(rbind, lapply(seq_along(which(ic)), function(i) xx))

            xcr <- crds[ic, c('longitude', 'latitude')]
            crds[ic, ] <- xx
            crds[ic, c('longitude', 'latitude')] <- xcr
            crds[ic, 'StatusX'] <- "red"
        }
    }

    #############
    crds[is.na(crds)] <- ""
    crds <- cbind(crds, LonX = crds[, 3], LatX = crds[, 4])
    ix <- crds[, 'LonX'] == "" | crds[, 'LatX'] == ""
    crds[ix, c('LonX', 'LatX')] <- NA
    crds <- as.data.frame(crds)
    crds$LonX <- as.numeric(as.character(crds$LonX))
    crds$LatX <- as.numeric(as.character(crds$LatX))

    #############
    # get parameters for each aws
    # crds$PARS <- pars

    #############
    out <- list(coords = crds, colinfo = colInfos, netinfo = netInfos)

    return(convJSON(out))
}

##############################

readCoordsDB <- function(aws_dir){
    on.exit(DBI::dbDisconnect(con_adt))

    con_adt <- connect.adt_db(aws_dir)
    if(is.null(con_adt)){
        return(convJSON(NULL))
    }

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")

    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    netFile <- file.path(dirJSON, "network_infos.json")
    netInfos <- jsonlite::read_json(netFile)
    net_seq <- names(netInfos)

    crdHdFile <- file.path(dirJSON, "coords_header_infos.json")
    nmCol <- jsonlite::read_json(crdHdFile)
    nmCol <- do.call(c, nmCol$header)
    nmCol <- c(nmCol, "network", "network_code", "startdate", "enddate")

    crds <- lapply(seq_along(netInfos), function(j){
        crd <- DBI::dbReadTable(con_adt, netInfos[[net_seq[j]]]$coords_table)
        crd$network <- netInfos[[net_seq[j]]]$name
        crd$network_code <- as.integer(netInfos[[net_seq[j]]]$code)

        return(crd)
    })

    crds <- lapply(crds, function(x) x[, nmCol, drop = FALSE])
    crds <- do.call(rbind, crds)

    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    return(crds)
}

#############
#' Get AWS coordinates for one network.
#'
#' Get AWS coordinates for one network to display on table.
#' 
#' @param network the AWS network code; 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

tableAWSCoords <- function(network, aws_dir){
    on.exit(DBI::dbDisconnect(con_adt))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    netFile <- file.path(dirJSON, "network_infos.json")
    netInfos <- jsonlite::read_json(netFile)
 
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    #############

    con_adt <- connect.adt_db(aws_dir)
    if(is.null(con_adt)){
        status <- data.frame(status = "unable to connect to database")
        return(convJSON(status))
    }

    crds <- DBI::dbReadTable(con_adt, netInfos[[network]]$coords_table)

    #############
    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    return(convJSON(crds))
}

#############
#' Get variables list.
#'
#' Read the list of variables to display on map.
#' 
#' @param time_step time step of the data: "hourly" or "daily".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readVariableListMap <- function(time_step, aws_dir){
    out <- readAWSParamsData(time_step, aws_dir)
    return(convJSON(out))
}

# # sapply(var_stats, function(x) sum(x$var_code == 5))
# # 75 83 89
# # LX0041-GABI LX0032-KIBU LX0031-NYAG

readAWSParamsData <- function(time_step, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    if(time_step %in% c('pentad', 'dekadal', 'monthly')){
        time_step <- 'daily'
    }
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
    awsPars <- readRDS(parsFile)

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        ret <- list(pars = NULL, status = 'no',
                    msg = "Unable to connect to ADT database")
        return(ret)
    }

    stats <- DBI::dbReadTable(conn, "adt_stats")

    var_names <- lapply(awsPars, '[[', 'params')
    var_names <- do.call(rbind, var_names)
    var_names <- var_names[!duplicated(var_names$code), , drop = FALSE]

    var_stats <- lapply(awsPars, '[[', 'stats')
    var_stats <- do.call(rbind, var_stats)
    dup <- duplicated(var_stats[, c('var_code', 'height', 'stat_code')])
    var_stats <- var_stats[!dup, , drop = FALSE]
    ist <- match(var_stats$stat_code, stats$code)
    var_stats$long_name <- stats$long_name[ist]
    var_hgt <- split(seq(nrow(var_stats)), paste0(var_stats$var_code, '-', var_stats$height))

    out <- lapply(var_hgt, function(ix){
        x <- var_stats[ix, , drop = FALSE]
        if(x$var_code[1] == 5){
            x <- x[x$stat_code == 4, , drop = FALSE]
        }
        vr <- var_names[var_names$code == x$var_code[1], , drop = FALSE]
        vr$height <- x$height[1]
        vr$stats <- list(x[, c('stat_code', 'stat_name', 'long_name'), drop = FALSE])
        vr
    })
    names(out) <- NULL

    v_order <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7)
    var_tab0 <- sapply(out, '[[', 'code')
    var_order <- orderedVariables(var_tab0, v_order)
    out <- out[var_order]

    if(time_step == 'daily'){
        # 10, 8, 14, 7, 15, 18 avg only
        # sum, avg, min, max only for others
        var_choices <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7, 15, 18)
        out <- lapply(out, function(x){
            if(!x$code %in% var_choices) return(NULL)
            ix <- x$code %in% c(8, 10, 9, 14, 7, 15, 18)
            xtmp <- x$stats[[1]]
            if(ix){
                # 9, 10, 8, 14, 7, 15, 18 avg only
                ip <- xtmp$stat_code == 1
            }else{
                # sum, avg, min, max only
                ip <- xtmp$stat_code %in% 1:4
            }
            x$stats[[1]] <- xtmp[ip, , drop = FALSE]
            return(x)
        })

        inull <- sapply(out, is.null)
        if(all(inull)){
            ret <- list(pars = NULL, status = 'no',
                        msg = "No available variable for daily data")
            return(ret)
        }

        out <- out[!inull]
    }

    ret <- list(vars = out, status = 'ok', msg = NULL)
    return(ret)
}

##############################
#' Get AWS Wind heights.
#'
#' Get AWS Wind heights for drop-down select.
#' 
#' @param time_step time step of the data: "minutes" or "hourly".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

getWindHeight <- function(time_step, aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
    awsPars <- readRDS(parsFile)

    wndHgt <- lapply(awsPars, function(x){
        if(all(9:10 %in% x$stats$var_code)){
            ivh <- x$stats$var_code %in% 9:10 & x$stats$stat_code == 1
            if(any(ivh)){
                y <- x$stats[ivh, , drop = FALSE]
                if(all(9:10 %in% y$var_code)){
                    dd_h <- sort(y$height[y$var_code == 9])
                    ff_h <- sort(y$height[y$var_code == 10])

                    if(length(ff_h) > length(dd_h)){
                        dd <- sapply(ff_h, function(v){
                            ii <- which.min(abs(dd_h - v))
                            dd_h[ii]
                        })
                        out <- list(wd_val = dd, ws_val = ff_h)
                    }else{
                        ff <- sapply(dd_h, function(v){
                            ii <- which.min(abs(ff_h - v))
                            ff_h[ii]
                        })
                        out <- list(wd_val = dd_h, ws_val = ff)
                    }

                    ## ZMD wind AWS height encoding 
                    frac <- out$ws_val %% 1
                    out$wnd_hgt <- round(out$ws_val, 1)
                    out$wnd_idx <- ((frac * 10) %% 1) * 10
                    out <- as.data.frame(out)
                    return(out)
                }
            }
        }

        return(NULL)
    })

    inull <- sapply(wndHgt, is.null)
    wndHgt <- wndHgt[!inull]
    wndHgt <- do.call(rbind, wndHgt)
    wndHgt <- wndHgt[!duplicated(wndHgt), , drop = FALSE]

    return(convJSON(wndHgt))
}

##############################
#' Get AWS Wind Metadata.
#'
#' Get AWS Wind metadata to display on map.
#' 
#' @param time_step time step of the data: "minutes" or "hourly".
#' @param height wind speed and direction heights above ground, format "<speedHeight>_<directionHeight>".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

readWindMetadata <- function(time_step, height, aws_dir){
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
    awsPars <- readRDS(parsFile)
    colFile <- file.path(dirJSON, "coords_header_infos.json")
    colInfos <- jsonlite::read_json(colFile)

    height <- strsplit(height, '_')
    ff_h <- as.numeric(height[[1]][1])
    dd_h <- as.numeric(height[[1]][2])

    wnd <- lapply(awsPars, function(x){
        iff <- x$stats$var_code == 10 & x$stats$height == ff_h & x$stats$stat_code == 1
        idd <- x$stats$var_code == 9 & x$stats$height == dd_h & x$stats$stat_code == 1

        if(any(iff) && any(idd)){
            x$params <- x$params[x$params$code %in% 9:10, , drop = FALSE]
            x$stats <- x$stats[iff | idd, , drop = FALSE]
            return(x)
        }

        return(NULL)
    })

    inull <- sapply(wnd, is.null)
    wnd <- wnd[!inull]

    names(wnd) <- sapply(wnd, function(x) paste0(x$coords$network_code, '_', x$coords$id))

    out <- list(data = wnd, colinfo = colInfos)

    return(convJSON(out))
}