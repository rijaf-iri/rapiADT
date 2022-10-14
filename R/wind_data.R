
wind_ffdd2uv <- function(ws, wd){
    wu <- -ws * sin(pi * wd / 180)
    wv <- -ws * cos(pi * wd / 180)
    return(cbind(wu, wv))
}

wind_uv2ffdd <- function(wu, wv){
    ff <- sqrt(wu^2 + wv^2)
    dd <- (atan2(wu, wv) * 180/pi) + ifelse(ff < 1e-14, 0, 180)
    return(cbind(ff, dd))
}

###########################

get_wind_data <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    timestep <- user_req$time_step

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    parsFile <- file.path(dirJSON, paste0("aws_parameters_", timestep, ".rds"))
    awsPars <- readRDS(parsFile)

    net_aws <- strsplit(user_req$net_aws, "_")[[1]]
    net_code <- as.integer(net_aws[1])
    aws_id <- net_aws[2]

    coords <- do.call(rbind, lapply(awsPars, '[[', "coords"))
    iaws <- which(coords$network_code == net_code & coords$id == aws_id)
    awsPars <- awsPars[[iaws]]

    wnd_hgt <- strsplit(user_req$height, "_")[[1]]
    ff_hgt <- as.numeric(wnd_hgt[1])
    dd_hgt <- as.numeric(wnd_hgt[2])

    time_frmt <- if(timestep == 'hourly') "%Y-%m-%d-%H" else "%Y-%m-%d-%H-%M"
    time_start <- strptime(user_req$start, time_frmt, tz = tz)
    time_end <- strptime(user_req$end, time_frmt, tz = tz)
    start <- as.numeric(time_start)
    end <- as.numeric(time_end)

    time_scale <- if(timestep == 'hourly') "Hourly" else "Minutes"
    ofrmt <- "%Y%m%d%H%M%S"
    filename <- paste(time_scale, "Wind-Data", awsPars$coords$name, awsPars$coords$network,
                      format(time_start, ofrmt), format(time_end, ofrmt), sep = '_')
    filename <- gsub(" ", "-", filename)

    msg_nodata <- paste('No available data. AWS:', awsPars$coords$name, awsPars$coords$network,
                        '; Period: from', time_start, 'to', time_end)

    OUT <- list(opts = list(status = msg_nodata, start = time_start, end = time_end,
                aws_id = aws_id, aws_name = awsPars$coords$name, net_code = net_code,
                net_name = awsPars$coords$network, filename = filename,
                height = round(ff_hgt, 1), tz = tz, time_step = timestep),
                data = NULL
            )
    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        OUT$opts$status <- 'Unable to connect to ADT database.'
        return(OUT)
    }

    #####
    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_minutes"
        qc_name <- "limit_check"
    }

    sel_col <- c('obs_time', 'value', qc_name)

    query_time <- list(colname_time = 'obs_time', start_time = start,
                       end_time = end, opr1 = ">=", opr2 = "<=")

    ##### speed
    query_ff <- list(network = net_code, id = aws_id, height = ff_hgt,
                       var_code = 10, stat_code = 1)
    query <- create_query_select(db_name, sel_col, query_ff, query_time)
    qres_ff <- DBI::dbGetQuery(conn, query)

    if(nrow(qres_ff) == 0){
        OUT$opts$status <- paste("Wind Speed.", OUT$opts$status)
        return(OUT)
    }

    ##### direction
    query_dd <- list(network = net_code, id = aws_id, height = dd_hgt,
                       var_code = 9, stat_code = 1)
    query <- create_query_select(db_name, sel_col, query_dd, query_time)
    qres_dd <- DBI::dbGetQuery(conn, query)

    if(nrow(qres_dd) == 0){
        OUT$opts$status <- paste("Wind Direction.", OUT$opts$status)
        return(OUT)
    }

    #####
    qres_ff$value[!is.na(qres_ff[, qc_name])] <- NA
    qres_dd$value[!is.na(qres_dd[, qc_name])] <- NA

    #####

    obs_time <- sort(unique(c(qres_ff$obs_time, qres_dd$obs_time)))

    ws <- qres_ff$value[match(obs_time, qres_ff$obs_time)]
    wd <- qres_dd$value[match(obs_time, qres_dd$obs_time)]

    ws_avail <- round(100 * sum(!is.na(ws)) / length(obs_time), 1)
    wd_avail <- round(100 * sum(!is.na(wd)) / length(obs_time), 1)

    OUT$data <- list(ws = ws, wd = wd, obs_time = obs_time,
                     ws_avail = ws_avail, wd_avail = wd_avail)
    OUT$opts$status <- 'plot'

    return(OUT)
}

