#' Get aggregated data.
#'
#' Get aggregated data to display on chart.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

chartAggrAWSData <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    out_ts <- get_one_aws_aggr_ts(user_req, aws_dir)
    return(convJSON(out_ts))
}

########

#' Get AWS aggregated spatial data.
#'
#' Get AWS aggregated spatial data to display on map.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

mapAggrAWSData <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    timestep <- user_req$time_step

    date_time <- get_aggrDataSP_date_time(timestep, user_req$date, tz)

    out_sp <- list(date = date_time$date, data = NULL, color = NULL,
                   key = NULL, status = 'no', msg = NULL,
                   pars = NULL, display = FALSE)

    ####
    awsPars <- readAWSParamsData(timestep, aws_dir)
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

    ####

    ckey_table <- switch(timestep, 'hourly' = 'adt_ckeyHour',
                         'daily' = 'adt_ckeyDay', 'pentad' = 'adt_ckeyPentad',
                         'dekadal' = 'adt_ckeyDekad', 'monthly' = 'adt_ckeyMonth')
    cKeys <- DBI::dbReadTable(conn, ckey_table)

    ####

    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_daily"
        qc_name <- "qc_check"
    }

    if(timestep %in% c('hourly', 'daily')){
        query_time <- list(colname_time = 'obs_time', start_time = date_time$time, opr1 = "=")
    }else{
        query_time <- list(colname_time = 'obs_time', start_time = date_time$time[1],
                           end_time = date_time$time[2], opr1 = ">=", opr2 = "<=")
    }

    query <- create_query_select(db_name, "*", query_time = query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out_sp$msg <- paste('No data for', date_time$date)
        return(convJSON(out_sp))
    }

    qres$value[!is.na(qres[, qc_name])] <- NA

    ivs <- paste(qres$var_code, qres$height, qres$stat_code, sep = '_')
    index <- split(seq(nrow(qres)), ivs)
    ipars <- paste(awsPars$code, awsPars$height, awsPars$stat_code, sep = '_')
    idx <- match(ipars, names(index))
    # cbind(ipars, names(index)[idx])
    idx <- idx[!is.na(idx)]
    index <- index[idx]

    if(length(index) == 0){
        out_sp$msg <- paste('No data for', date_time$date)
        return(convJSON(out_sp))
    }

    #####
    if(timestep %in% c('hourly', 'daily')){
        donP <- lapply(index, function(ix){
            x <- qres[ix, , drop = FALSE]
            x_id <- paste0(x$network, '_', x$id)
            ic <- match(aws_id, x_id)
            x$value[ic]
        })
    }else{
        ### aggregate
        minFrac <- DBI::dbReadTable(conn, "adt_minfrac_aggregate")
        daty <- gsub('-', '', user_req$date)

        donP <- aggregateSP_PenDekMon(qres, daty, timestep, aws_id, index, minFrac)

        if(is.null(donP)){
            out_sp$msg <- paste('No data for', date_time$date)
            return(convJSON(out_sp))
        }
    }

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

#' Get aggregated data.
#'
#' Get aggregated data to display on table.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

tableAggrAWSData <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    out <- get_allvar_aws_aggr_ts(user_req, aws_dir)
    if(is.null(out$data)){
        out$status <- 'no'
    }else{
        out$status <- 'ok'
        don <- out$data
        ina <- apply(don, 2, function(x) sum(!is.na(x)) > 0)
        var_head <- c(NA, out$var[ina[-1]])
        don <- don[, ina, drop = FALSE]
        out$data <- don
        out$var <- var_head
    }

    return(convJSON(out))
}

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

downAWSAggrOneCSV <- function(user_req, aws_dir){
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
    if(user_req$user$userlevel == 2){
        user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
        user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
        if(!((net_code %in% user_net) & (aws_id %in% user_id))){
            out$csv_data <- "You are not allowed to download data from this station"
            return(convJSON(out))
        }
    }

    #####
    out_ts <- get_one_aws_aggr_ts(user_req, aws_dir)
    out$filename <- paste0(out_ts$opts$filename, '.csv')

    if(out_ts$opts$status != "plot"){
        out$csv_data <- out_ts$opts$status
        return(convJSON(out))
    }else{
        don <- out_ts$data
        daty <- as.POSIXct(don[, 1]/1000, origin = origin, tz = tz)
        daty <- format_aggregate_dates(daty, user_req$time_step)

        if(out_ts$opts$arearange){
            nom <- c("date", "min", "max", "avg")
        }else{
            nom <- c("date", out_ts$opts$stat)
        }

        mat <- don[, -1, drop = FALSE]
        ina <- apply(mat, 1, function(n) sum(!is.na(n)) > 0)
        don <- don[ina, , drop = FALSE]
        daty <- daty[ina]

        don <- as.data.frame(don)
        names(don) <- nom
        don$date <- daty

        out$csv_data <- convCSV(don)
        return(convJSON(out))
    }
}

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

downAWSAggrDataCSV <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    out <- get_allvar_aws_aggr_ts(user_req, aws_dir)
    if(is.null(out$data)){
        filename <- paste0("download_", user_req$net_aws, ".csv")
        csv_data <- out$msg
    }else{
        net_aws <- strsplit(user_req$net_aws, "_")[[1]]
        start <- out$data$date[1]
        end <- out$data$date[nrow(out$data)]
        filename <- paste0(net_aws[2], '_all-variables_', start, '_', end, '.csv')
        don <- out$data
        ina <- apply(don, 2, function(x) sum(!is.na(x)) > 0)
        var_head <- c(NA, out$var[ina[-1]])
        don <- don[, ina, drop = FALSE]
        don <- rbind(var_head, names(don), as.matrix(don))
        csv_data <- convCSV(don, col.names = FALSE)
    }
    out <- list(filename = filename, csv_data = csv_data)

    return(convJSON(out))
}

#' Get aggregated data in CDT format.
#'
#' Get aggregated data in CDT format for download.
#' 
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSAggrCDTCSV <- function(user_req, aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    var_hgt <- strsplit(user_req$var_hgt, "_")[[1]]
    var_code <- as.integer(var_hgt[1])
    height <- as.numeric(var_hgt[2])
    stat_code <- as.integer(user_req$stat)
    timestep <- user_req$time_step

    #####
    out <- list(filename = NULL, csv_data = NULL)
    out$filename <- "download_CDT_data.csv"

    if(user_req$user$uid < 0){
        out$csv_data <- "You are not allowed to download data"
        return(convJSON(out))
    }

    if(user_req$user$useraction == 1){
        out$csv_data <- "You are not allowed to download data"
        return(convJSON(out))
    }

    #####
    awsPars <- readAWSParamsData(timestep, aws_dir)
    if(awsPars$status == 'no'){
        out$csv_data <- awsPars$msg
        return(convJSON(out))
    }

    awsPars <- lapply(awsPars$vars, function(x){
        vr <- x[, c('code', 'name', 'units', 'height'), drop = FALSE]
        st <- x$stats[[1]]
        cbind(vr[rep(1, nrow(st)), ], st)
    })
    awsPars <- do.call(rbind, awsPars)
    ip <- awsPars$code == var_code & awsPars$height == height & awsPars$stat_code == stat_code
    upar <- awsPars[ip, , drop = FALSE]
    var_name <- paste0(upar$name, '_', upar$long_name, '_at_', upar$height, 'm')
    var_name <- gsub(' ', '-', var_name)
    start_d <- gsub('-', '', user_req$start)
    end_d <- gsub('-', '', user_req$end)

    out$filename <- paste0('CDTdata_', timestep, '_', var_name, '_', start_d, '_', end_d, '.csv')

    #####

    crds <- readCoordsDB(aws_dir)
    crds <- crds[, c('id', 'network_code', 'longitude', 'latitude', 'altitude')]
    crds$aws_id <- paste0(crds$id, '_', crds$network_code)
    crds <- crds[, c('aws_id', 'longitude', 'latitude', 'altitude')]

    #####
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        out$csv_data <- 'Unable to connect to ADT database.'
        return(convJSON(out))
    }

    wind_data <- FALSE
    var_select <- var_code
    if(timestep == 'hourly'){
        db_name <- "aws_hourly"
        qc_name <- "spatial_check"
    }else{
        db_name <- "aws_daily"
        qc_name <- "qc_check"
        if(timestep != 'daily' & (var_code %in% 9:10)){
            var_select <- 9:10
            wind_data <- TRUE
        }
    }

    #####
    start_end <- get_aggrDataTS_start_end_dates(timestep, user_req$start, user_req$end, tz)
    start <- as.numeric(start_end[1])
    end <- as.numeric(start_end[2])

    #####
    query_args <- list(height = height, var_code = var_select, stat_code = stat_code)
    query_time <- list(colname_time = 'obs_time', start_time = start,
                       end_time = end, opr1 = ">=", opr2 = "<=")
    sel_col <- c('network', 'id', 'var_code', 'obs_time', 'value', qc_name)
    query <- create_query_select(db_name, sel_col, query_args, query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0){
        out$csv_data <- 'No available data.'
        return(convJSON(out))
    }

    qres$value[!is.na(qres[, qc_name])] <- NA
    qres$aws_net <- paste0(qres$id, '_', qres$network)

    #####

    if(user_req$user$userlevel == 2){
        user_net <- sapply(user_req$user$awslist$aws, '[[', 'network_code')
        user_id <- sapply(user_req$user$awslist$aws, '[[', 'aws_id')
        user_ix <- paste0(user_id, '_', user_net)
        ix <- match(user_ix, qres$aws_net)
        ix <- ix[!is.na(ix)]
        qres <- qres[ix, , drop = FALSE]

        if(nrow(qres) == 0){
            out$csv_data <- 'No available data.'
            return(convJSON(out))
        }
    }

    #####

    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_aggregate")

    if(wind_data){
        ff <- qres[qres$var_code == 10, , drop = FALSE]
        dd <- qres[qres$var_code == 9, , drop = FALSE]

        ws <- NULL
        if(nrow(ff) > 0){
            ws <- reshape2::acast(ff, obs_time~aws_net, mean, value.var = 'value')
            ws[is.nan(ws)] <- NA
            ff_id <- colnames(ws)
            ff_date <- as.integer(rownames(ws))
            ws <- as.matrix(ws)
            dimnames(ws) <- NULL
        }
        wd <- NULL
        if(nrow(dd) > 0){
            wd <- reshape2::acast(dd, obs_time~aws_net, mean, value.var = 'value')
            wd[is.nan(wd)] <- NA
            dd_id <- colnames(wd)
            dd_date <- as.integer(rownames(wd))
            wd <- as.matrix(wd)
            dimnames(wd) <- NULL
        }

        ##### 
        minFrac <- minFrac[, c('var_code', paste0('min_frac_', timestep))]
        min_frac <- minFrac[minFrac$var_code == 10, 2]

        ##### 
        if(!is.null(ws) & !is.null(wd)){
            daty <- intersect(ff_date, dd_date)
            odaty <- as.Date(daty, origin = origin)
            ff <- match(daty, ff_date)
            dd <- match(daty, dd_date)

            aws_id <- intersect(ff_id, dd_id)
            iff <- match(aws_id, ff_id)
            idd <- match(aws_id, dd_id)
            nc <- length(aws_id)

            ws <- ws[ff, iff, drop = FALSE]
            wd <- wd[dd, idd, drop = FALSE]

            uv <- wind_ffdd2uv(ws, wd)

            wu <- uv[, 1:nc, drop = FALSE]
            wv <- uv[, nc + (1:nc), drop = FALSE]
            wu <- aggregateCDT_PenDekMon(wu, odaty, timestep, stat_code, min_frac)
            wv <- aggregateCDT_PenDekMon(wv, odaty, timestep, stat_code, min_frac)

            uv <- wind_uv2ffdd(wu$data, wv$data)

            odaty <- wu$dates
            if(var_code == 10){
                don <- uv[, 1:nc, drop = FALSE]
            }else{
                don <- uv[, nc + (1:nc), drop = FALSE]
            }
        }else if(!is.null(ws) & is.null(wd) & var_code == 10){
            odaty <- as.Date(ff_date, origin = origin)
            aws_id <- ff_id
            aggr <- aggregateCDT_PenDekMon(ws, odaty, timestep, stat_code, min_frac)
            don <- aggr$data
            odaty <- aggr$dates
        }else{
            out$csv_data <- 'No available data.'
            return(convJSON(out))
        }
    }else{
        don <- reshape2::acast(qres, obs_time~aws_net, mean, value.var = 'value')
        don[is.nan(don)] <- NA
        aws_id <- colnames(don)
        daty <- as.integer(rownames(don))
        don <- as.matrix(don)
        dimnames(don) <- NULL

        if(timestep == 'hourly'){
            odaty <- as.POSIXct(daty, origin = origin, tz = tz)
        }else{
            odaty <- as.Date(daty, origin = origin)
        }

        if(timestep %in% c("pentad", "dekadal", "monthly")){
            minFrac <- minFrac[, c('var_code', paste0('min_frac_', timestep))]
            min_frac <- minFrac[minFrac$var_code == var_code, 2]

            aggr <- aggregateCDT_PenDekMon(don, odaty, timestep, stat_code, min_frac)
            don <- aggr$data
            odaty <- aggr$dates
        }
    }

    odaty <- format_aggregate_dates(odaty, timestep)

    iaws <- match(aws_id, crds$aws_id)
    coords <- crds[iaws, , drop = FALSE]
    coords <- t(as.matrix(coords))

    ## remove aws all NA
    ina <- colSums(!is.na(don)) > 0
    if(!any(ina)){
        out$csv_data <- 'No available data.'
        return(convJSON(out))
    }
    don <- don[, ina, drop = FALSE]
    coords <- coords[, ina, drop = FALSE]

    ## remove dates all NA
    ina <- rowSums(!is.na(don)) > 0
    don <- don[ina, , drop = FALSE]
    odaty <- odaty[ina]

    don <- round(don, 2)

    capt <- c('ID', 'LON', 'LAT', 'ELV')
    don <- rbind(cbind(capt, coords), cbind(odaty, don))
    out$csv_data <- convCSV(don, col.names = FALSE)

    return(convJSON(out))
}
