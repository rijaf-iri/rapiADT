#' Get minutes data.
#'
#' Get minutes data to display on chart.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

chartMinAWSData <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]
    var_hgt <- strsplit(user_req$var_hgt, "_")[[1]]
    var_code <- as.integer(var_hgt[1])
    height <- as.numeric(var_hgt[2])
    stat_code <- as.integer(user_req$stat)
    plotrange <- as.logical(as.integer(user_req$plotrange))

    start <- strptime(user_req$start, "%Y-%m-%d-%H-%M", tz = tz)
    start <- as.numeric(start)
    end <- strptime(user_req$end, "%Y-%m-%d-%H-%M", tz = tz)
    end <- as.numeric(end)

    #####
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, "aws_parameters_minutes.rds")
    awsPars <- readRDS(parsFile)
    coords <- do.call(rbind, lapply(awsPars, '[[', "coords"))
    iaws <- which(coords$network_code == net_code & coords$id == aws_id)

    awsPars <- awsPars[[iaws]]

    iv <- awsPars$params$code == var_code
    ist <- awsPars$stats$var_code == var_code &
           awsPars$stats$height == height &
           awsPars$stats$stat_code == stat_code
    var_name <- paste(awsPars$params$name[iv], 'at', paste0(height, 'm'))
    var_ylab <- paste(awsPars$params$name[iv],
                paste0('[', awsPars$params$units[iv], ']'))
    
    var_stat <- if(plotrange) "min-avg-max" else awsPars$stats$stat_name[ist]
    var_stat1 <- paste('Statistic:', var_stat)
    plot_name <- paste0(awsPars$params$name[iv], " [", awsPars$stats$stat_name[ist], "]")
    var_stn <- paste("Station:", awsPars$coords$name, '-',
                     awsPars$coords$id, '-', awsPars$coords$network)
    titre <- paste(var_name, var_stat1, sep = '; ')
    filename <- paste(var_name, var_stat, awsPars$coords$id,
                      awsPars$coords$network, sep = '_')
    filename <- gsub(" ", "-", filename)

    msg_nodata <- paste('No available data.', var_stn)

    OUT <- list(opts = list(title = titre, subtitle = var_stn,
                var = var_code, arearange = FALSE, 
                status = msg_nodata, name = 'none',
                tz = tz, filename = filename), 
                data = NULL, display_data = NULL,
                yaxis = list(at = NA, tick = NA, mtick = NA,
                             ymax = NA, ylab = var_ylab)
            )

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        OUT$opts$status <- 'Unable to connect to ADT database.'
        return(convJSON(OUT))
    }

    query_stat <- if(plotrange) 1:3 else stat_code
    query_args <- list(network = net_code, id = aws_id, height = height,
                       var_code = var_code, stat_code = query_stat)
    query_time <- list(colname_time = 'obs_time', start_time = start,
                       end_time = end, opr1 = ">=", opr2 = "<=")
    sel_col <- c('stat_code', 'obs_time', 'value', 'limit_check')
    query <- create_query_select("aws_minutes", sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(convJSON(OUT))

    qres$value[!is.na(qres$limit_check)] <- NA

    ####

    plotR <- FALSE
    if(plotrange){
        db_vorder <- c('Ave', 'Min', 'Max')
        qres <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
        qres[is.nan(qres)] <- NA
        c_qres <- as.integer(dimnames(qres)[[2]])
        c_qres <- db_vorder[c_qres]
        r_qres <- as.integer(dimnames(qres)[[1]])

        qres <- data.frame(r_qres, qres)
        names(qres) <- c("obs_time", c_qres)
        rvars <- c("Min", "Max", "Ave")

        if(all(rvars %in% names(qres))){
            plotR <- TRUE
            qres <- qres[, c("obs_time", rvars), drop = FALSE]
        }else{
            ist <- db_vorder[stat_code]
            if(ist %in% names(qres)){
                qres <- qres[, c("obs_time", ist), drop = FALSE]
                OUT$opts$title <- gsub("min-avg-max", ist, OUT$opts$title)
                OUT$opts$filename <- gsub("min-avg-max", ist, OUT$opts$filename)
            }else{
                OUT$opts$status <- paste("No min-avg-max statistics available", var_stn)
                return(convJSON(OUT))
            }
        }
    }else{
        qres <- qres[, c("obs_time", "value"), drop = FALSE]
    }

    ####
    qres <- qres[order(qres$obs_time), , drop = FALSE]
    don <- qres[, -1, drop = FALSE]
    daty <- as.POSIXct(qres$obs_time, origin = origin, tz = tz)

    ####
    ddif <- diff(daty)
    tu <- attributes(ddif)$units
    fac <- switch(tu, "secs" = 1/60, "mins" = 1, "hours" = 60,
                  "days" = 60 * 24, "weeks" = 60 * 24 * 7)
    ddif <- ddif * fac

    query <- create_query_select("aws_timestep", "timestep",
                            list(code = net_code, id = aws_id))
    timestep <- DBI::dbGetQuery(conn, query)
    timestep <- if(is.na(timestep$timestep)) 3 * 60 else timestep$timestep

    idt <- which(ddif > timestep)
    if(length(idt) > 0){
        miss.daty <- daty[idt] + timestep * 60
        miss.daty <- format(miss.daty, "%Y%m%d%H%M%S", tz = tz)
        daty1 <- rep(NA, length(daty) + length(miss.daty))
        don1 <- data.frame(stat = rep(NA, length(daty1)))

        if(plotR){
            don1 <- cbind(don1, don1, don1)
            names(don1) <- c('Min', 'Max', 'Ave')
        }

        daty1[idt + seq(length(miss.daty))] <- miss.daty
        ix <- is.na(daty1)
        daty1[ix] <- format(daty, "%Y%m%d%H%M%S", tz = tz)
        don1[ix, ] <- don

        daty <- strptime(daty1, "%Y%m%d%H%M%S", tz = tz)
        don <- don1
    }

    ####
    ## convert to millisecond
    time <- 1000 * as.numeric(as.POSIXct(daty))

    if(plotR){
        don <- as.matrix(cbind(time, don[, c('Min', 'Max', 'Ave')]))
        dimnames(don) <- NULL

        OUT$data <- don
        OUT$opts$name <- c("Range", "Average")
    }else{
        don <- as.matrix(cbind(time, don))
        dimnames(don) <- NULL

        OUT$data <- don
        OUT$opts$name <- plot_name
    }

    OUT$opts$arearange <- plotR
    OUT$opts$status <- 'plot'

    ####

    display_data <- FALSE
    if(user_req$user$uid > 0){
        display_data <- TRUE
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
            if(!((net_code %in% user_net) & (aws_id %in% user_id))){
                display_data <- FALSE
            }
        }
    }

    OUT$display_data <- display_data

    brks <- pretty(OUT$data[, -1, drop = FALSE])
    OUT$yaxis$tick <- brks

    if(!display_data){
        tmp <- OUT$data[, -1, drop = FALSE]
        mx <- max(tmp, na.rm = TRUE)
        mn <- min(tmp, na.rm = TRUE)
        OUT$data[, -1] <- (tmp - mn)/(mx - mn)
        brks <- (brks - mn)/(mx - mn)
    }

    OUT$yaxis$at <- brks
    OUT$yaxis$mtick <- (brks[2] - brks[1])/2
    OUT$yaxis$ymax <- brks[length(brks)] + OUT$yaxis$mtick/4

    return(convJSON(OUT))
}

#' Get hourly data.
#'
#' Get hourly data to display on map.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

mapHourAWSData <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    daty <- strptime(user_req$time, "%Y-%m-%d-%H-%M", tz = tz)
    time <- as.numeric(daty)
    daty <- format(daty, "%Y-%m-%d %H:%M:%S")

    out_sp <- list(date = daty, data = NULL, color = NULL,
                   key = NULL, status = 'no', msg = NULL,
                   pars = NULL, display = FALSE)

    ####
    awsPars <- readAWSParamsData("hourly", aws_dir)
    if(awsPars$status == 'no'){
        out_sp$msg <- awsPars$msg
        return(convJSON(out_sp))
    }

    awsPars <- lapply(awsPars$vars, function(x){
        vr <- x[, c('code', 'name', 'units', 'height'), drop = FALSE]
        st <- x$stats[[1]]
        cbind(vr[rep(1, nrow(st)), ], st)
    })
    awsPars <- do.call(rbind, awsPars)

    crds <- readCoordsDB(aws_dir)
    crds <- crds[, c('id', 'name', 'longitude', 'latitude', 'network', 'network_code')]

    aws_id <- paste0(crds$network_code, '_', crds$id)

    ####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        out_sp$msg <- 'Unable to connect to ADT database.'
        return(convJSON(out_sp))
    }

    cKeys <- DBI::dbReadTable(conn, 'adt_ckeyHour')

    ####

    query_time <- list(colname_time = 'obs_time', start_time = time, opr1 = "=")
    query <- create_query_select("aws_hourly", "*", query_time = query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out_sp$msg <- paste('No data for', daty)
        return(convJSON(out_sp))
    }

    qres$value[!is.na(qres$spatial_check)] <- NA

    ivs <- paste(qres$var_code, qres$height, qres$stat_code, sep = '_')
    index <- split(seq(nrow(qres)), ivs)

    donP <- lapply(index, function(ix){
        x <- qres[ix, , drop = FALSE]
        x_id <- paste0(x$network, '_', x$id)
        ic <- match(aws_id, x_id)
        x$value[ic]
    })

    #####
    donC <- lapply(seq_along(donP), function(j){
        x <- strsplit(names(donP[j]), '_')[[1]]
        x <- as.numeric(x)
        ip <- awsPars$code == x[1] & awsPars$height == x[2] & awsPars$stat_code == x[3]
        pars <- awsPars[ip, , drop = FALSE]
        if(nrow(pars) == 0){
            pars <- awsPars[awsPars$code == x[1], , drop = FALSE]
            pars <- pars[1, , drop = FALSE]
        }
        ckey <- cKeys[cKeys$var_code == x[1], , drop = FALSE]
        cols <- paste0('col', 1:14)
        kols <- ckey[ckey$ckey_args == "colors", cols]
        kols <- as.character(kols)
        kols <- kols[!is.na(kols)]
        brks <- ckey[ckey$ckey_args == "breaks", cols]
        brks <- as.numeric(as.character(brks))
        brks <- brks[!is.na(brks)]
        if(length(brks) == 0){
            brks <- pretty(donP[[j]])
        }

        userOp <- list(uColors = list(custom = TRUE, color = kols),
                       levels = list(custom = TRUE, levels = brks, equidist = TRUE))
        ck <- image.plot_Legend_pars(NULL, userOp)
        ckey <- list(labels = ck$legend.axis$labels, colors = ck$colors)
        ckey$title <- paste(pars$name, "-", pars$long_name, paste0('(', pars$units, ')'))

        zmin <- suppressWarnings(min(donP[[j]], na.rm = TRUE))
        if(!is.infinite(zmin)){
            ck$breaks[1] <- ifelse(ck$breaks[1] > zmin, zmin, ck$breaks[1])
        }
        zmax <- suppressWarnings(max(donP[[j]], na.rm = TRUE))
        if(!is.infinite(zmax)){
            nl <- length(ck$breaks)
            ck$breaks[nl] <- ifelse(ck$breaks[nl] < zmax, zmax, ck$breaks[nl])
        }
        kolor <- ck$colors[findInterval(donP[[j]], ck$breaks, rightmost.closed = TRUE, left.open = TRUE)]

        ## zero set to white: precip, wind
        if(x[1] %in% c(5, 9, 10)){
            ix <- !is.na(donP[[j]]) & donP[[j]] == 0
            kolor[ix] <- "#FFFFFF"
        }

        list(kolor = kolor, ckey = ckey)
    })

    #####
    kolor <- lapply(donC, '[[', 'kolor')
    names(kolor) <- names(donP)
    out_sp$color <- kolor

    ckeys <- lapply(donC, '[[', 'ckey')
    names(ckeys) <- names(donP)
    out_sp$key <- ckeys

    rownames(awsPars) <- NULL
    out_sp$pars <- awsPars

    donP <- do.call(cbind, donP)
    donP <- as.data.frame(donP)
    donP$user_aws <- rep(0, nrow(donP))

    wnd_dd <- strsplit(names(donP), '_')
    wnd_dd <- sapply(wnd_dd, '[[', 1) != '9'
    iusr <- !names(donP) %in% 'user_aws'

    display_data <- FALSE
    if(user_req$user$uid > 0){
        display_data <- TRUE
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
            user_ix <- paste0(user_net, '_', user_id)

            iu <- aws_id %in% user_ix
            donP[!iu, wnd_dd & iusr] <- NA
            donP$user_aws[iu] <- 1
        }else{
            donP$user_aws <- 2
        }
    }else{
        donP[, wnd_dd & iusr] <- NA
    }

    out_sp$display <- display_data
    out_sp$data <- cbind(crds, donP)
    out_sp$status <- 'ok'

    return(convJSON(out_sp))
}

##########
#' Download all variables for one AWS.
#'
#' Download all variables for one AWS.
#' 
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSMinDataCSV <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]

    #####
    out <- list(filename = paste0("download_", user_req$net_aws, ".csv"), csv_data = NULL)
    out$csv_data <- "You are not allowed to download data from this station"
    if(user_req$user$uid > 0){
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
            if(!((net_code %in% user_net) & (aws_id %in% user_id))){
                return(convJSON(out))
            }
        }
    }else{
        return(convJSON(out))
    }

    #####
    start <- strptime(user_req$start, "%Y-%m-%d-%H-%M", tz = tz)
    time0 <- as.numeric(start)
    end <- strptime(user_req$end, "%Y-%m-%d-%H-%M", tz = tz)
    time1 <- as.numeric(end)

    start <- format(start, "%Y%m%d%H%M00")
    end <- format(end, "%Y%m%d%H%M00")
    out$filename <- paste0(aws_id, '_all-variables_', start, '_', end, '.csv')

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        out$csv_data <- 'Unable to connect to ADT database.'
        return(convJSON(out))
    }

    query_args <- list(network = net_code, id = aws_id)
    query_time <- list(colname_time = 'obs_time', start_time = time0,
                       end_time = time1, opr1 = ">=", opr2 = "<=")
    sel_col <- c('height', 'var_code', 'stat_code', 'obs_time', 'value', 'limit_check')
    query <- create_query_select("aws_minutes", sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out$csv_data <- 'No available data.'
        return(convJSON(out))
    }

    qres$value[!is.na(qres$limit_check)] <- NA

    daty <- sort(unique(qres$obs_time))

    ixvar <- paste0(qres$var_code, '_', qres$height, '_', qres$stat_code)
    index <- split(seq_along(ixvar), ixvar)
    var_nom <- names(index)

    don <- lapply(index, function(ix){
        x <- qres[ix, c('obs_time', 'value'), drop = FALSE]
        if(nrow(x) == 0) return(rep(NA, length(daty)))
        x$value[match(daty, x$obs_time)]
    })
    don <- do.call(cbind, don)

    pars <- DBI::dbReadTable(conn, "adt_pars")
    ip <- paste0(pars$var_code, '_', pars$stat_code)
    cvar <- do.call(rbind, strsplit(var_nom, '_'))
    ic <- paste0(cvar[, 1], '_', cvar[, 3])
    pars <- pars[match(ic, ip), , drop = FALSE]

    v_order <- c(5, 2, 6, 3, 1, 8, 10, 9, 14, 7)
    var_order <- orderedVariables(pars$var_code, v_order)

    pars <- pars[var_order, , drop = FALSE]
    var_nom <- var_nom[var_order]
    cvar <- cvar[var_order, , drop = FALSE]
    don <- don[, var_order, drop = FALSE]

    var_name <- paste0(pars$var_name, " (", pars$var_stat, ") at ",
                       cvar[, 2], "m [units: ", pars$var_units, "]")
    info_pars <- rbind(c("", var_name), c("Time", var_nom))

    daty <- as.POSIXct(daty, origin = origin, tz = tz)
    daty <- format(daty, "%Y-%m-%d %H:%M:%S")
    don <- cbind(daty, don)
    don <- rbind(info_pars, don)
    
    out$csv_data <- convCSV(don, col.names = FALSE)

    return(convJSON(out))
}


##########
#' Download displayed observation for one AWS.
#'
#' Download the time series of the displayed observation for one AWS.
#' 
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSMinOneCSV <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]

    #####
    out <- list(filename = paste0("download_", user_req$net_aws, ".csv"), csv_data = NULL)
    out$csv_data <- "You are not allowed to download data from this station"
    if(user_req$user$uid > 0){
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
            if(!((net_code %in% user_net) & (aws_id %in% user_id))){
                return(convJSON(out))
            }
        }
    }else{
        return(convJSON(out))
    }

    #####

    var_hgt <- strsplit(user_req$var_hgt, "_")[[1]]
    var_code <- as.integer(var_hgt[1])
    height <- as.numeric(var_hgt[2])
    stat_code <- as.integer(user_req$stat)
    plotrange <- as.logical(as.integer(user_req$plotrange))

    start <- strptime(user_req$start, "%Y-%m-%d-%H-%M", tz = tz)
    time0 <- as.numeric(start)
    end <- strptime(user_req$end, "%Y-%m-%d-%H-%M", tz = tz)
    time1 <- as.numeric(end)
    start <- format(start, "%Y%m%d%H%M00")
    end <- format(end, "%Y%m%d%H%M00")

    #####
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, "aws_parameters_minutes.rds")
    awsPars <- readRDS(parsFile)
    coords <- do.call(rbind, lapply(awsPars, '[[', "coords"))
    iaws <- which(coords$network_code == net_code & coords$id == aws_id)
    awsPars <- awsPars[[iaws]]
    iv <- awsPars$params$code == var_code
    ist <- awsPars$stats$var_code == var_code &
           awsPars$stats$height == height &
           awsPars$stats$stat_code == stat_code
    var_name <- paste(awsPars$params$name[iv], 'at', paste0(height, 'm'))
    var_stat <- if(plotrange) "min-avg-max" else awsPars$stats$stat_name[ist]
    var_name <- gsub(' ', '-', var_name)

    out$filename <- paste0(aws_id, '_', var_name, '_', var_stat, '_', start, '_', end, '.csv')

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        out$csv_data <- 'Unable to connect to ADT database.'
        return(convJSON(out))
    }

    query_stat <- if(plotrange) 1:3 else stat_code
    query_args <- list(network = net_code, id = aws_id, height = height,
                       var_code = var_code, stat_code = query_stat)
    query_time <- list(colname_time = 'obs_time', start_time = time0,
                       end_time = time1, opr1 = ">=", opr2 = "<=")
    sel_col <- c('stat_code', 'obs_time', 'value', 'limit_check')
    query <- create_query_select("aws_minutes", sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out$csv_data <- paste('No available data for', start, '-', end)
        return(convJSON(out))
    }

    qres$value[!is.na(qres$limit_check)] <- NA
    qres <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
    qres[is.na(qres) | is.nan(qres)] <- ''

    daty <- as.POSIXct(as.numeric(rownames(qres)), origin = origin, tz = tz)
    daty <- format(daty, "%Y-%m-%d %H:%M:%S")
    ist <- awsPars$stats$var_code == var_code & awsPars$stats$height == height
    nmcol <- awsPars$stats[ist, , drop = FALSE]
    im <- match(as.integer(colnames(qres)), nmcol$stat_code)
    qres <- data.frame(daty, qres)
    names(qres) <- c('time', nmcol$stat_name[im])
    rownames(qres) <- NULL
    out$csv_data <- convCSV(qres)

    return(convJSON(out))
}
