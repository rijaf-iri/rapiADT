
get_one_aws_aggr_ts <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]
    var_hgt <- strsplit(user_req$var_hgt, "_")[[1]]
    var_code <- as.integer(var_hgt[1])
    height <- as.numeric(var_hgt[2])
    stat_code <- as.integer(user_req$stat)
    plotrange <- as.logical(as.integer(user_req$plotrange))
    timestep <- user_req$time_step

    #####
    time_step <- if(timestep == 'hourly') "hourly" else "daily"

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
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
    time_scale <- switch(timestep,
                        'hourly' = "Hourly",
                        'daily' = "Daily",
                        'pentad' = "Pentadal",
                        'dekadal' = "Dekadal",
                        'monthly' = "Monthly")
    titre <- paste0(paste(time_scale, var_name), '; ', var_stat1)

    filename <- paste(time_scale, var_name, var_stat, awsPars$coords$id,
                      awsPars$coords$network, sep = '_')
    filename <- gsub(" ", "-", filename)

    msg_nodata <- paste('No available data.', var_stn)

    OUT <- list(opts = list(title = titre, subtitle = var_stn,
                var = var_code, stat = var_stat, arearange = FALSE, 
                status = msg_nodata, name = 'none',
                tz = tz, filename = filename), 
                data = NULL, display_data = NULL,
                yaxis = list(at = NA, tick = NA, mtick = NA,
                             ymax = NA, ylab = var_ylab)
            )

    #####

    start_end <- get_aggrDataTS_start_end_dates(timestep, user_req$start, user_req$end, tz)
    start <- as.numeric(start_end[1])
    end <- as.numeric(start_end[2])

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        OUT$opts$status <- 'Unable to connect to ADT database.'
        return(OUT)
    }

    #####
    wind_data <- FALSE
    var_select <- var_code
    col_select <- c('stat_code', 'obs_time', 'value')
    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_daily"
        qc_name <- "qc_check"
        if(timestep != 'daily' & (var_code %in% 9:10)){
            var_select <- 9:10
            wind_data <- TRUE
            plotrange <- FALSE
            col_select <- c('var_code', col_select)
        }
    }

    #####
    query_stat <- if(plotrange) 1:3 else stat_code
    query_args <- list(network = net_code, id = aws_id, height = height,
                       var_code = var_select, stat_code = query_stat)
    query_time <- list(colname_time = 'obs_time', start_time = start,
                       end_time = end, opr1 = ">=", opr2 = "<=")
    sel_col <- c(col_select, qc_name)
    query <- create_query_select(db_name, sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(OUT)

    qres$value[!is.na(qres[, qc_name])] <- NA

    ####

    plotR <- FALSE
    if(plotrange){
        db_vorder <- c('Avg', 'Min', 'Max')
        qres <- reshape2::acast(qres, obs_time~stat_code, mean, value.var = 'value')
        qres[is.nan(qres)] <- NA
        c_qres <- as.integer(dimnames(qres)[[2]])
        c_qres <- db_vorder[c_qres]
        r_qres <- as.integer(dimnames(qres)[[1]])

        qres <- data.frame(r_qres, qres)
        names(qres) <- c("obs_time", c_qres)
        rvars <- c("Min", "Max", "Avg")

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
                return(OUT)
            }
        }
    }else{
        if(wind_data){
            qres <- reshape2::acast(qres, obs_time~var_code, mean, value.var = 'value')
            qres[is.nan(qres)] <- NA
            c_qres <- dimnames(qres)[[2]]
            r_qres <- as.integer(dimnames(qres)[[1]])
            qres <- data.frame(r_qres, qres)
            names(qres) <- c("obs_time", c_qres)
        }else{
            qres <- qres[, c("obs_time", "value"), drop = FALSE]
            names(qres) <- c("obs_time", tools::toTitleCase(var_stat))
        }
    }

    ######
    qres <- qres[order(qres$obs_time), , drop = FALSE]
    don <- qres[, -1, drop = FALSE]

    if(timestep == "hourly"){
        daty <- as.POSIXct(qres$obs_time, origin = origin, tz = tz)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'hour')
    }else{
        daty <- as.Date(qres$obs_time, origin = origin)
        odaty <- daty
        seq_daty <- seq(min(daty), max(daty), 'day')
    }

    if(timestep %in% c("pentad", "dekadal", "monthly")){
        minFrac <- DBI::dbReadTable(conn, "adt_minfrac_aggregate")
        aggr_ts <- aggregateTS_PenDekMon(don, daty, timestep,
                                         var_code, wind_data, minFrac)
        don <- aggr_ts$data
        seq_daty <- aggr_ts$sdates
        odaty <- aggr_ts$odates
    }

    ######

    it <- match(seq_daty, odaty)
    don <- don[it, , drop = FALSE]
    don <- as.matrix(don)

    if(all(is.na(don))) return(OUT)

    time <- 1000 * as.numeric(as.POSIXct(seq_daty))
    don <- cbind(time, don)
    dimnames(don) <- NULL

    OUT$data <- don
    OUT$opts$name <- if(plotR) c("Range", "Average") else plot_name
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

    return(OUT)
}

########

get_allvar_aws_aggr_ts <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]
    timestep <- user_req$time_step

    #####
    out <- list(data = NULL, var = NULL, msg = NULL)
    out$msg <- "You are not allowed to download data from this station"

    if(user_req$user$uid > 0){
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
            if(!((net_code %in% user_net) & (aws_id %in% user_id))){
                return(out)
            }
        }
    }else{
        return(out)
    }

    #####
    start_end <- get_aggrDataTS_start_end_dates(timestep, user_req$start, user_req$end, tz)
    start <- as.numeric(start_end[1])
    end <- as.numeric(start_end[2])

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        out$msg <- 'Unable to connect to ADT database.'
        return(out)
    }

    #####
    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_daily"
        qc_name <- "qc_check"
    }

    #####
    query_args <- list(network = net_code, id = aws_id)
    query_time <- list(colname_time = 'obs_time', start_time = start,
                       end_time = end, opr1 = ">=", opr2 = "<=")
    sel_col <- c('height', 'var_code', 'stat_code', 'obs_time', 'value', qc_name)
    query <- create_query_select(db_name, sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out$msg <- 'No available data.'
        return(out)
    }

    qres$value[!is.na(qres[, qc_name])] <- NA

    #####
    awsPars <- readAWSParamsData(timestep, aws_dir)
    if(awsPars$status == 'no'){
        out$msg <- awsPars$msg
        return(out)
    }

    awsPars <- lapply(awsPars$vars, function(x){
        vr <- x[, c('code', 'name', 'units', 'height'), drop = FALSE]
        st <- x$stats[[1]]
        cbind(vr[rep(1, nrow(st)), ], st)
    })
    awsPars <- do.call(rbind, awsPars)
    pvars <- paste0(awsPars$code, '_', awsPars$height, '_', awsPars$stat_code)

    #####
    daty <- sort(unique(qres$obs_time))
    ixvar <- paste0(qres$var_code, '_', qres$height, '_', qres$stat_code)
    index <- split(seq_along(ixvar), ixvar)
    var_nom <- names(index)

    var_ix <- match(pvars, var_nom)
    ina <- !is.na(var_ix)
    var_ix <- var_ix[ina]
    awsPars <- awsPars[ina, , drop = FALSE]
    index <- index[var_ix]
    var_nom <- var_nom[var_ix]

    don <- lapply(index, function(ix){
        x <- qres[ix, c('obs_time', 'value'), drop = FALSE]
        if(nrow(x) == 0) return(rep(NA, length(daty)))
        x$value[match(daty, x$obs_time)]
    })
    don <- do.call(cbind, don)
    if(timestep == 'hourly'){
        odaty <- as.POSIXct(daty, origin = origin, tz = tz)
    }else{
        odaty <- as.Date(daty, origin = origin)
    }

    if(timestep %in% c("pentad", "dekadal", "monthly")){
        minFrac <- DBI::dbReadTable(conn, "adt_minfrac_aggregate")
        minFrac <- minFrac[, c('var_code', paste0('min_frac_', timestep))]

        iv <- awsPars$code
        iv[iv %in% 9:11] <- 10
        min_frac <- minFrac[match(iv, minFrac$var_code), 2]

        dates <- as.Date(daty, origin = origin)
        index_ts <- aggregateTS_PenDekMon_Index(dates, timestep)

        tmp <- lapply(seq_along(index_ts$index), function(j){
            x <- don[index_ts$index[[j]], , drop = FALSE]
            res <- rep(NA, length(var_nom))

            avail_frac <- index_ts$nobs[j]/index_ts$nobs0[j]
            rna <- avail_frac >= min_frac
            cna <- colSums(!is.na(x))/index_ts$nobs0[j] >= min_frac
            ina <- rna & cna
            if(!any(ina)) return(res)
            x <- x[, ina, drop = FALSE]
            vpr <- awsPars[ina, , drop = FALSE]
            iwnd <- vpr$code %in% 9:11

            # non wind
            donP1 <- NULL
            if(any(!iwnd)){
                y <- x[, !iwnd, drop = FALSE]
                v <- vpr[!iwnd, , drop = FALSE]
                donP1 <- lapply(seq(ncol(y)), function(i){
                    fun <- switch(v$stat_name[i], "sum" = sum, mean)
                    # fun <- switch(v$stat_name[i], "sum" = sum, "avg" = mean,
                    #               "min" = min, "max" = max, mean)
                    fun(y[, i], na.rm = TRUE)
                })
                donP1 <- do.call(cbind, donP1)
                colnames(donP1) <- dimnames(y)[[2]]
            }

            # wind data
            donP2 <- NULL
            if(any(iwnd)){
                y <- x[, iwnd, drop = FALSE]
                v <- vpr[iwnd, , drop = FALSE]
                ihgt <- split(seq(ncol(y)), v$height)

                donP2 <- lapply(ihgt, function(ih){
                    y1 <- y[, ih, drop = FALSE]
                    v1 <- v[ih, , drop = FALSE]
                    wndL <- 9:10 %in% v1$code
                    w <- y1[1, , drop = FALSE]
                    w[] <- NA
                    if(all(wndL)){
                        iws <- v1$code == 10
                        iwd <- v1$code == 9
                        uv <- wind_ffdd2uv(y1[, iws], y1[, iwd])
                        uv <- colMeans(uv, na.rm = TRUE)
                        wo <- wind_uv2ffdd(uv[1], uv[2])
                        w[, iws] <- wo[1]
                        w[, iwd] <- wo[2]
                        return(w)
                    }else if(!wndL[1] & wndL[2]){
                        iws <- v1$code == 10
                        w[, iws] <- mean(y1[, iws], na.rm = TRUE)
                        return(w)
                    }else{
                        return(w)
                    }
                })
                donP2 <- do.call(cbind, donP2)
            }

            cbind(donP1, donP2)
        })

        tmp <- do.call(rbind, tmp)
        im <- match(var_nom, dimnames(tmp)[[2]])
        don <- tmp[, im, drop = FALSE]
        colnames(don) <- var_nom

        odaty <- index_ts$odates
    }

    odaty <- format_aggregate_dates(odaty, timestep)

    don <- as.data.frame(round(don, 2))
    don <- cbind(date = odaty, don)
    var_name <- paste0(awsPars$name, " (", awsPars$stat_name, ") at ",
                       awsPars$height, "m [units: ", awsPars$units, "]")

    out$data <- don
    out$var <- var_name

    return(out)
}

########

aggregateTS_PenDekMon <- function(don, dates, time_step, var_code,
                                  wind_data, minFrac)
{
    minFrac <- minFrac[, c('var_code', paste0('min_frac_', time_step))]
    iv <- if(var_code %in% 9:11) 10 else var_code
    min_frac <- minFrac[minFrac$var_code == iv, 2]
    index_ts <- aggregateTS_PenDekMon_Index(dates, time_step)
    avail_frac <- index_ts$nobs/index_ts$nobs0
    ina <- avail_frac >= min_frac

    xout <- don[1, , drop = FALSE]
    xout[] <- NA
    xout <- xout[rep(1, length(index_ts$index)), , drop = FALSE]
    if(wind_data){
        xout <- xout[, as.character(var_code), drop = FALSE]
        names(xout) <- 'Avg'
    }

    index <- index_ts$index[ina]
    nobs0 <- index_ts$nobs0[ina]

    tmp <- lapply(seq_along(index), function(j){
        x <- don[index[[j]], , drop = FALSE]
        if(wind_data){
            ## wind data 9 & 10
            wndL <- c('9', '10') %in% names(x)
            agg <- data.frame(Avg = NA)
            if(all(wndL)){
                iff <- sum(!is.na(x[, '10']))/nobs0[j] < min_frac
                idd <- sum(!is.na(x[, '9']))/nobs0[j] < min_frac
                if(iff | idd) return(agg)
                uv <- wind_ffdd2uv(x[, '10'], x[, '9'])
                uv <- colMeans(uv, na.rm = TRUE)
                wo <- wind_uv2ffdd(uv[1], uv[2])
                if(var_code == 9){
                    agg$Avg <- wo[2]
                }else if(var_code == 10){
                    agg$Avg <- wo[1]
                }else{
                    agg$Avg <- NA
                }
                return(agg)
            }else if(!wndL[1] & wndL[2] & var_code == 10){
                y <- x[, '10']
                ia <- is.na(y)
                if(sum(!ia)/nobs0[j] < min_frac) return(agg)
                agg$Avg <- mean(y, na.rm = TRUE)
                return(agg)
            }else{
                return(agg)
            }
        }else{
            agg <- lapply(names(x), function(n){
                fun <- if(n == 'Sum') sum else mean
                # fun <- switch(n, "Sum" = sum, "Avg" = mean,
                #              "Min" = min, "Max" = max, mean)
                y <- x[, n]
                ia <- is.na(y)
                if(all(ia)) return(NA)
                if(sum(!ia)/nobs0[j] < min_frac) return(NA)
                fun(y, na.rm = TRUE)
            })
            agg <- do.call(cbind.data.frame, agg)
            names(agg) <- names(x)
            return(agg)
        }
    })

    xout[ina, ] <- do.call(rbind, tmp)
    out <- list(data = xout, sdates = index_ts$sdates,
                odates = index_ts$odates)
    return(out)
}

########

aggregateSP_PenDekMon <- function(qres, dates, time_step,
                                  aws_id, index, minFrac)
{
    minFrac <- minFrac[, c('var_code', paste0('min_frac_', time_step))]
    nobs <- switch(time_step,
                'pentad' = nb_day_of_pentad(dates),
                'dekadal' = nb_day_of_dekad(dates),
                'monthly' = nb_day_of_month(dates))
    vhs <- do.call(rbind, strsplit(names(index), '_'))
    ## wind
    ivhs <- vhs[, 1] %in% c('9', '10')

    ## no wind variables
    donP1 <- NULL
    if(any(!ivhs)){
        donP1 <- lapply(index[!ivhs], function(ix){
            x <- qres[ix, , drop = FALSE]
            var_code <- x$var_code[1]
            stat_code <- x$stat_code[1]
            istn <- split(seq(nrow(x)), paste0(x$network, '_', x$id))
            avail_frac <- sapply(istn, length)/nobs
            ina <- avail_frac >= minFrac[minFrac$var_code == var_code, 2]
            if(!any(ina)) return(rep(NA, length(aws_id)))

            y <- lapply(istn[ina], function(s){
                fun <- if(stat_code == 4) sum else mean
                fun(x$value[s], na.rm = TRUE)
            })
            ic <- match(aws_id, names(y))
            y <- unname(do.call(c, y))
            y[ic]
        })
    }

    ## wind
    donP2 <- NULL
    if(any(ivhs)){
        vhs <- vhs[ivhs, , drop = FALSE]
        index <- index[ivhs]
        ihgt <- split(seq(nrow(vhs)), vhs[, 2])
        min_frac <- minFrac[minFrac$var_code == 10, 2]
        donW <- lapply(ihgt, function(h){
            vhs1 <- vhs[h, , drop = FALSE]
            idx <- index[h]
            iff <- vhs1[, 1] == "10"
            if(!any(iff)) return(NULL)
            ff <- qres[idx[iff][[1]], , drop = FALSE]
            idd <- vhs1[, 1] == "9"
            # no direction
            if(!any(idd)){
                # compute mean ff
                istn <- split(seq(nrow(ff)), paste0(ff$network, '_', ff$id))
                avail_frac <- sapply(istn, length)/nobs
                ina <- avail_frac >= min_frac
                if(!any(ina)) return(NULL)

                y <- lapply(istn[ina], function(s){
                    mean(ff$value[s], na.rm = TRUE)
                })
                ic <- match(aws_id, names(y))
                y <- unname(do.call(c, y))
                out_ff <- list(y[ic])
                names(out_ff) <- names(idx[iff])
                return(out_ff)
            }else{
                ## compute vector mean, ff, dd
                dd <- qres[idx[idd][[1]], , drop = FALSE]
                iwff <- split(seq(nrow(ff)), paste0(ff$network, '_', ff$id))
                iwdd <- split(seq(nrow(dd)), paste0(dd$network, '_', dd$id))

                nmff <- names(iwff)
                nmdd <- names(iwdd)
                intwnd <- intersect(nmff, nmdd)

                # aws with speed only
                aws_ff <- !nmff %in% nmdd
                wnd_spd <- NULL
                if(any(aws_ff) | length(intwnd) == 0){
                    # no common aws for speed & direction
                    if(length(intwnd) == 0){
                       aws_ff <- rep(TRUE, length(iwff))
                    }
                    avail_frac <- sapply(iwff[aws_ff], length)/nobs
                    ina <- avail_frac >= min_frac
                    if(any(ina)){
                        y <- lapply(iwff[ina], function(s){
                            mean(ff$value[s], na.rm = TRUE)
                        })

                        if(length(intwnd) == 0){
                            ic <- match(aws_id, names(y))
                            y <- unname(do.call(c, y))
                            out_ff <- list(y[ic])
                            names(out_ff) <- names(idx[iff])
                            return(out_ff)
                        }else{
                            wnd_spd <- y
                        }
                    }else{
                        if(length(intwnd) == 0) return(NULL)
                    }
                }

                iwff <- iwff[match(intwnd, nmff)]
                iwdd <- iwdd[match(intwnd, nmdd)]

                wnd_dat <- lapply(seq_along(iwff), function(j){
                    ws <- ff[iwff[[j]], c('obs_time', 'value'), drop = FALSE]
                    wd <- dd[iwdd[[j]], c('obs_time', 'value'), drop = FALSE]
                    wnd <- merge(ws, wd, by.x = 'obs_time', by.y = 'obs_time', all = TRUE)

                    ws_farc <- length(iwff[[j]])/nobs
                    wd_frac <- length(iwdd[[j]])/nobs
                    ws_na <- ws_farc >= min_frac
                    wd_na <- wd_frac >= min_frac

                    if(!ws_na & !wd_na){
                        return(c(NA, NA))
                    }else if(!ws_na & wd_na){
                        return(c(NA, NA))
                    }else if(ws_na & !wd_na){
                        return(c(mean(ws$value, na.rm = TRUE), NA))
                    }else{
                        uv <- wind_ffdd2uv(wnd$value.x, wnd$value.y)
                        uv <- colMeans(uv, na.rm = TRUE)
                        wo <- wind_uv2ffdd(uv[1], uv[2])
                        return(c(wo[1], wo[2]))
                    }
                })
                ws <- lapply(wnd_dat, '[[', 1)
                names(ws) <- intwnd
                ws <- c(ws, wnd_spd)
                wd <- lapply(wnd_dat, '[[', 2)
                names(wd) <- intwnd

                ics <- match(aws_id, names(ws))
                ws <- unname(do.call(c, ws))
                out_ff <- ws[ics]

                icd <- match(aws_id, names(wd))
                wd <- unname(do.call(c, wd))
                out_dd <- wd[icd]

                out_wnd <- list(out_ff, out_dd)
                names(out_wnd) <- c(names(idx[iff]), names(idx[idd]))
                return(out_wnd)
            }
        })
    
        inull <- sapply(donW, is.null)
        if(any(!inull)){
            donW <- donW[!inull]
            nomL <- lapply(donW, function(x) names(x))
            nomL <- do.call(c, nomL)
            donP2 <- unlist(donW , recursive = FALSE)
            names(donP2) <- nomL
        }
    }

    donP <- c(donP1, donP2)
    ## remove all NA
    ina <- sapply(donP, function(x) sum(!is.na(x)) > 0)
    donP <- donP[ina]
    if(length(donP) == 0) donP <- NULL

    return(donP)
}

########

aggregateCDT_PenDekMon <- function(don, dates, time_step,
                                   stat_code, min_frac)
{
    index_ts <- aggregateTS_PenDekMon_Index(dates, time_step)

    avail_frac <- index_ts$nobs/index_ts$nobs0
    ina <- avail_frac >= min_frac

    xout <- don[1, , drop = FALSE]
    xout[] <- NA
    xout <- xout[rep(1, length(index_ts$index)), , drop = FALSE]

    index <- index_ts$index[ina]
    nobs0 <- index_ts$nobs0[ina]

    tmp <- lapply(seq_along(index), function(j){
        x <- don[index[[j]], , drop = FALSE]
        xna <- colSums(!is.na(x))/nobs0[j] >= min_frac
        y <- rep(NA, ncol(x))
        if(!any(xna)) return(y)
        fun <- if(stat_code == 4) colSums else colMeans
        y[xna] <- fun(x[, xna, drop = FALSE], na.rm = TRUE)
        return(y)
    })

    xout[ina, ] <- do.call(rbind, tmp)
    out <- list(data = xout, dates = index_ts$odates)

    return(out)
}
