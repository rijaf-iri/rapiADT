
#' Get aggregated data.
#'
#' Get aggregated data to display on chart for multiple AWS.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

chartAggrAWSDataSel <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    out_ts <- get_sel_aws_aggr_ts(user_req, aws_dir)
    out_ts$aws_list <- NULL
    if(out_ts$opts$out != "no"){
        time <- 1000 * as.numeric(as.POSIXct(out_ts$date))
        out_ts$date <- time
    }

    return(convJSON(out_ts))
}

#' Get aggregated data.
#'
#' Get aggregated data for selected stations to display on table.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

tableAggrAWSDataSel <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    out <- format_sel_aws_aggr_ts(user_req, aws_dir)
    return(convJSON(out))
}

#' Download aggregated data.
#'
#' Download aggregated data for a selected stations.
#' 
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSAggrDataSelCSV <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    out_ts <- format_sel_aws_aggr_ts(user_req, aws_dir)
    filename <- paste0(out_ts$filename, '.csv')
    out <- list(filename = filename, csv_data = NULL)
    if(out_ts$out == "no"){
        out$csv_data <- out_ts$status[[1]]$msg
    }else{
        out$csv_data <- convCSV(out_ts$data)
    }

    return(convJSON(out))
}

format_sel_aws_aggr_ts <- function(user_req, aws_dir){
    if(user_req$user$userlevel == 2){
        user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
        user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
        user_aws <- paste0(user_net, '_', user_id)
    }

    out_ts <- get_sel_aws_aggr_ts(user_req, aws_dir)
    out <- list(out = out_ts$opts$out, status = out_ts$opts$status,
                data = NULL, filename = out_ts$opts$filename)

    if(out_ts$opts$out != "no"){
        don <- lapply(out_ts$data, '[[', 'data')
        aws_list <- out_ts$aws_list
        if(user_req$user$userlevel == 2){
            dat_aws <- paste0(aws_list$network_code, '_', aws_list$id)
            iusr <- dat_aws %in% user_aws
            if(!any(iusr)){
                out$out <- 'no'
                out$status <- NULL
                out$status[[1]]$type <- "error"
                msg <- 'You are not allowed to display or download'
                msg <- paste(msg, 'data from the selected stations')
                out$status[[1]]$msg <- msg
                return(out)
            }

            don <- don[iusr]
            aws_list <- aws_list[iusr, , drop = FALSE]
        }

        don <- do.call(cbind, don)
        aws_list <- aws_list[, 1:3, drop = FALSE]
        daty <- format_aggregate_dates(out_ts$date, user_req$time_step)

        ina <- apply(don, 1, function(n) sum(!is.na(n)) > 0)
        don <- don[ina, , drop = FALSE]
        daty <- daty[ina]

        hdr <- cbind(c('ID', 'NETWORK'), t(aws_list[, 2:3]))
        don <- cbind(daty, don)
        don <- rbind(hdr, don)
        dimnames(don) <- NULL
        don <- as.data.frame(don)
        names(don) <- c('', aws_list[, 1])

        out$data <- don
    }

    return(out)
}

get_sel_aws_aggr_ts <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    network_awsid <- do.call(c, user_req$net_aws)
    net_aws <- do.call(rbind, strsplit(network_awsid, "_"))
    net_code <- as.integer(net_aws[, 1])
    aws_id <- net_aws[, 2]
    var_hgt <- strsplit(user_req$var_hgt, "_")[[1]]
    var_code <- as.integer(var_hgt[1])
    height <- as.numeric(var_hgt[2])
    stat_code <- as.integer(user_req$stat)
    timestep <- user_req$time_step

    #####
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")

    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    time_step <- if(timestep == 'hourly') "hourly" else "daily"
    parsFile <- file.path(dirJSON, paste0("aws_parameters_", time_step, ".rds"))
    awsPars <- readRDS(parsFile)
    coords <- do.call(rbind, lapply(awsPars, '[[', "coords"))
    iaws <- which(coords$network_code == net_code[1] & coords$id == aws_id[1])

    iv <- awsPars[[iaws]]$params$code == var_code
    ist <- awsPars[[iaws]]$stats$var_code == var_code &
           awsPars[[iaws]]$stats$height == height &
           awsPars[[iaws]]$stats$stat_code == stat_code
    var_name <- paste(awsPars[[iaws]]$params$name[iv], 'at', paste0(height, 'm'))
    var_ylab <- paste(awsPars[[iaws]]$params$name[iv],
                paste0('[', awsPars[[iaws]]$params$units[iv], ']'))
    var_stat <- awsPars[[iaws]]$stats$stat_name[ist]
    titre_stat <- paste('Statistic:', var_stat)
    time_scale <- switch(timestep,
                        'hourly' = "Hourly",
                        'daily' = "Daily",
                        'pentad' = "Pentadal",
                        'dekadal' = "Dekadal",
                        'monthly' = "Monthly")
    titre <- paste0(paste(time_scale, var_name), '; ', titre_stat)

    filename <- paste(time_scale, var_name, var_stat, sep = '_')
    filename <- paste0(filename, '_selected_aws')
    filename <- gsub(" ", "-", filename)

    coords <- lapply(seq_along(network_awsid), function(j){
        iaws <- which(coords$network_code == net_code[j] & coords$id == aws_id[j])
        awsPars[[iaws]]$coords
    })

    out_aws_crd <- lapply(coords, function(x){
        x[, c('name', 'id', 'network', 'network_code'), drop = FALSE]
    })
    out_aws_crd <- do.call(rbind, out_aws_crd)

    OUT <- list(opts = list(title = titre, var = var_code, stat = var_stat,
                status = list(NULL), out = NULL, tz = tz, filename = filename), 
                data = NULL, date = NULL, aws_list = out_aws_crd, 
                yaxis = list(at = NA, tick = NA, mtick = NA, ymax = NA, ylab = var_ylab)
            )

    #####
    start_end <- get_aggrDataTS_start_end_dates(timestep, user_req$start, user_req$end, tz)
    start <- as.numeric(start_end[1])
    end <- as.numeric(start_end[2])

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        OUT$opts$out <- 'no'
        OUT$opts$status[[1]]$type <- "error"
        OUT$opts$status[[1]]$msg <- 'Unable to connect to ADT database.'
        return(OUT)
    }

    #####
    wind_data <- FALSE
    var_select <- var_code
    col_select <- c('obs_time', 'value')
    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_daily"
        qc_name <- "qc_check"
        if(timestep != 'daily' & (var_code %in% 9:10)){
            var_select <- 9:10
            wind_data <- TRUE
            col_select <- c('var_code', col_select)
        }
    }

    #####
    qres <- lapply(seq_along(network_awsid), function(j){
        query_args <- list(network = net_code[j], id = aws_id[j], height = height,
                           var_code = var_select, stat_code = stat_code)
        query_time <- list(colname_time = 'obs_time', start_time = start,
                           end_time = end, opr1 = ">=", opr2 = "<=")
        sel_col <- c(col_select, qc_name)
        query <- create_query_select(db_name, sel_col, query_args, query_time)
        res <- DBI::dbGetQuery(conn, query)
        if(nrow(res) == 0) return(NULL)
        res$value[!is.na(res[, qc_name])] <- NA
        return(res)
    })

    inull <- sapply(qres, is.null)
    if(all(inull)){
        OUT$opts$out <- 'no'
        OUT$opts$status[[1]]$type <- "error"
        OUT$opts$status[[1]]$msg <- 'No available data for all selected stations.'
        return(OUT)
    }

    if(any(inull)){
        OUT$opts$out <- 'part'
        OUT$opts$status <- lapply(coords[inull], function(s){
            msg <- paste("No available data for",
                         s$name, "-", s$id, "-", s$network)
            list(type = 'warning', msg = msg)
        })

        qres <- qres[!inull]
        coords <- coords[!inull]
        OUT$aws_list <- OUT$aws_list[!inull, , drop = FALSE]
    }

    qres <- lapply(qres, function(x){
        if(wind_data){
            y <- reshape2::acast(x, obs_time~var_code, mean, value.var = 'value')
            y[is.nan(y)] <- NA
            c_qres <- dimnames(y)[[2]]
            r_qres <- as.integer(dimnames(y)[[1]])
            y <- data.frame(r_qres, y)
            names(y) <- c("obs_time", c_qres)
        }else{
            y <- x[, c("obs_time", "value"), drop = FALSE]
            names(y) <- c("obs_time", tools::toTitleCase(var_stat))
        }

        y <- y[order(y$obs_time), , drop = FALSE]
        return(y)
    })

    if(timestep %in% c("pentad", "dekadal", "monthly")){
        minFrac <- DBI::dbReadTable(conn, "adt_minfrac_aggregate")
        aggr_ts <- lapply(qres, function(x){
            daty <- as.Date(x$obs_time, origin = origin)
            don <- x[, -1, drop = FALSE]
            aggregateTS_PenDekMon(don, daty, timestep,
                            var_code, wind_data, minFrac)
        })

        seq_daty <- lapply(aggr_ts, '[[', 'sdates')
        seq_daty <- do.call(c, seq_daty)
        seq_daty <- sort(unique(seq_daty))

        don <- lapply(aggr_ts, function(x){
            it <- match(seq_daty, x$odates)
            x$data[, 1][it]
        })

        #####
        min_frac <- minFrac[, c('var_code', paste0('min_frac_', timestep))]
        iv <- if(var_code %in% 9:11) 10 else var_code
        min_frac <- min_frac[min_frac$var_code == iv, 2]

        #####
        inull <- sapply(don, function(x) sum(!is.na(x)) == 0)
        if(all(inull)){
            OUT$opts$out <- 'no'
            tmp <- lapply(coords, function(s){
                msg <- paste("No enough data to compute", timestep, "data for",
                             s$name, "-", s$id, "-", s$network,
                             ", using minimum fraction:", min_frac)
                list(type = 'warning', msg = msg)
            })

            if(length(OUT$opts$status) > 0){
                OUT$opts$status <- c(OUT$opts$status, tmp)
            }else{
                OUT$opts$status <- tmp
            }
            return(OUT)
        }

        if(any(inull)){
            OUT$opts$out <- 'part'
            tmp <- lapply(coords[inull], function(s){
                msg <- paste("No enough data to compute", timestep, "data for",
                             s$name, "-", s$id, "-", s$network,
                             ", using minimum fraction:", min_frac)
                list(type = 'warning', msg = msg)
            })

            if(!is.null(OUT$opts$status[[1]]$type)){
                OUT$opts$status <- c(OUT$opts$status, tmp)
            }else{
                OUT$opts$status <- tmp
            }
            don <- don[!inull]
            coords <- coords[!inull]
            OUT$aws_list <- OUT$aws_list[!inull, , drop = FALSE]
        }
    }else{
        obs_time <- lapply(qres, '[[', 'obs_time')
        obs_time <- do.call(c, obs_time)
        obs_time <- sort(unique(obs_time))
        range_time <- range(obs_time, na.rm = TRUE)

        if(timestep == "hourly"){
            rg_daty <- as.POSIXct(range_time, origin = origin, tz = tz)
            seq_daty <- seq(rg_daty[1], rg_daty[2], 'hour')
        }else{
            rg_daty <- as.Date(range_time, origin = origin)
            seq_daty <- seq(rg_daty[1], rg_daty[2], 'day')
        }

        don <- lapply(qres, function(x){
            if(timestep == "hourly"){
                daty <- as.POSIXct(x$obs_time, origin = origin, tz = tz)
            }else{
                daty <- as.Date(x$obs_time, origin = origin)
            }
        
            it <- match(seq_daty, daty)
            x[it, 2]
        })
    }

    don <- lapply(don, function(x) round(x, 2))

    ####

    brks <- pretty(do.call(c, don))
    OUT$yaxis$tick <- brks
    OUT$yaxis$at <- brks
    OUT$yaxis$mtick <- (brks[2] - brks[1])/2
    OUT$yaxis$ymax <- brks[length(brks)] + OUT$yaxis$mtick/4

    ####

    if(user_req$user$uid > 0){
        if(user_req$user$userlevel == 2){
            user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
            user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
        }
    }

    kolor <- fields::tim.colors(length(don))

    don <- lapply(seq_along(don), function(j){
        name <- paste0(coords[[j]]$name, " [ID = " , coords[[j]]$id,
                       " ; ", coords[[j]]$network, "]")
        display_data <- FALSE
        if(user_req$user$uid > 0){
            display_data <- TRUE
            if(user_req$user$userlevel == 2){
                if(!((coords[[j]]$network_code %in% user_net) &
                    (coords[[j]]$id %in% user_id)))
                {
                    display_data <- FALSE
                }
            }
        }

        list(name = name, color = kolor[j], data = don[[j]],
             display = display_data)
    })

    OUT$opts$out <- if(is.null(OUT$opts$out)) 'ok' else OUT$opts$out
    OUT$date <- seq_daty
    OUT$data <- don

    return(OUT)
}


