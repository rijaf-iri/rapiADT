
char_utc2local_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_utc2local_char <- function(dates, format, tz){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

char_local2utc_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

time_local2utc_time <- function(dates){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_local2utc_char(dates, format)
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_utc2time_local <- function(dates, tz){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_utc2local_char(dates, format, tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

day_of_month <- function(year, mon){
    daty <- paste(year, mon, 28:31, sep = '-')
    daty <- as.Date(daty)
    rev((28:31)[!is.na(daty)])[1]
}

nb_day_of_month <- function(daty){
    nbm <- mapply(day_of_month,
                  substr(daty, 1, 4),
                  substr(daty, 5, 6),
                  USE.NAMES = FALSE)
    as.numeric(nbm)
}

nb_day_of_pentad <- function(daty){
    day <- as.numeric(substr(daty, 7, 7))
    nbp <- rep(5, length(daty))
    nbp[day >= 6] <- nb_day_of_month(daty[day == 6]) - 25
    return(nbp)
}

nb_day_of_dekad <- function(daty){
    day <- as.numeric(substr(daty, 7, 7))
    nbd <- rep(10, length(daty))
    nbd[day == 3] <- nb_day_of_month(daty[day == 3]) - 20
    return(nbd)
}

get_aggrDataTS_start_end_dates <- function(time_step, start, end, tz){
    start_end <- switch(time_step,
                        'hourly' = local({
                                    frmt <- "%Y-%m-%d-%H"
                                    start <- strptime(start, frmt, tz = tz)
                                    end <- strptime(end, frmt, tz = tz)
                                    c(start, end)
                                }),
                        'daily' = local({
                                    frmt <- "%Y-%m-%d"
                                    start <- as.Date(start, frmt)
                                    end <- as.Date(end, frmt)
                                    c(start, end)
                                }),
                        'pentad' = local({
                                    frmt <- "%Y-%m-%d"
                                    pen1 <- strsplit(start, '-')[[1]]
                                    i1 <- as.integer(pen1[3])
                                    pen1[3] <- c(1, 6, 11, 16, 21, 26)[i1]
                                    start <- as.Date(paste0(pen1, collapse = '-'), frmt)
                                    pen2 <- strsplit(end, '-')[[1]]
                                    i2 <- as.integer(pen2[3])
                                    penE <- c(5, 10, 15, 20, 25, 30)
                                    if(i2 == 6){
                                        dpn <- gsub('-', '', end)
                                        penE[6] <- 25 + nb_day_of_pentad(dpn)
                                    }
                                    pen2[3] <- penE[i2]
                                    end <- as.Date(paste0(pen2, collapse = '-'), frmt)
                                    c(start, end)
                                }),
                        'dekadal' = local({
                                    frmt <- "%Y-%m-%d"
                                    dek1 <- strsplit(start, '-')[[1]]
                                    i1 <- as.integer(dek1[3])
                                    dek1[3] <- c(1, 11, 21)[i1]
                                    start <- as.Date(paste0(dek1, collapse = '-'), frmt)
                                    dek2 <- strsplit(end, '-')[[1]]
                                    i2 <- as.integer(dek2[3])
                                    dekE <- c(10, 20, 30)
                                    if(i2 == 3){
                                        ddk <- gsub('-', '', end)
                                        dekE[3] <- 20 + nb_day_of_dekad(ddk)
                                    }
                                    dek2[3] <- dekE[i2]
                                    end <- as.Date(paste0(dek2, collapse = '-'), frmt)
                                    c(start, end)
                                }),
                        'monthly' = local({
                                    start <- as.Date(paste0(start, '-01'))
                                    end_date <- as.Date(paste0(end, '-15'))
                                    end_date <- format(end_date, '%Y%m%d')
                                    nbday <- nb_day_of_month(end_date)
                                    end <- as.Date(paste0(end, '-', nbday))
                                    c(start, end)
                                })
                       )
    return(start_end)
}

get_aggrDataSP_date_time <- function(time_step, date, tz){
    date_time <- switch(time_step,
                        'hourly' = local({
                                    frmt <- "%Y-%m-%d-%H"
                                    daty <- strptime(date, frmt, tz = tz)
                                    time <- as.numeric(daty)
                                    daty <- format(daty, "%Y-%m-%d %H:%M")
                                    list(date = daty, time = time)
                                }),
                        'daily' = local({
                                    frmt <- "%Y-%m-%d"
                                    daty <- as.Date(date, frmt)
                                    time <- as.numeric(daty)
                                    daty <- format(daty, "%Y-%m-%d")
                                    list(date = daty, time = time)
                                }),
                        'pentad' = local({
                                    frmt <- "%Y-%m-%d"
                                    pen <- strsplit(date, '-')[[1]]
                                    it <- as.integer(pen[3])
                                    pen1 <- pen
                                    pen1[3] <- c(1, 6, 11, 16, 21, 26)[it]
                                    start <- as.Date(paste0(pen1, collapse = '-'), frmt)
                                    start <- as.numeric(start)
                                    pen2 <- pen
                                    penE <- c(5, 10, 15, 20, 25, 30)
                                    if(it == 6){
                                        dpn <- gsub('-', '', date)
                                        penE[6] <- 25 + nb_day_of_pentad(dpn)
                                    }
                                    pen2[3] <- penE[it]
                                    end <- as.Date(paste0(pen2, collapse = '-'), frmt)
                                    end <- as.numeric(end)
                                    daty <- paste(pen[1], pen[2], paste0('pen', pen[3]), sep = '-')
                                    list(date = daty, time = c(start, end))
                                }),
                        'dekadal' = local({
                                    frmt <- "%Y-%m-%d"
                                    dek <- strsplit(date, '-')[[1]]
                                    it <- as.integer(dek[3])
                                    dek1 <- dek
                                    dek1[3] <- c(1, 11, 21)[it]
                                    start <- as.Date(paste0(dek1, collapse = '-'), frmt)
                                    start <- as.numeric(start)
                                    dek2 <- dek
                                    dekE <- c(10, 20, 30)
                                    if(it == 3){
                                        ddk <- gsub('-', '', date)
                                        dekE[3] <- 20 + nb_day_of_dekad(ddk)
                                    }
                                    dek2[3] <- dekE[it]
                                    end <- as.Date(paste0(dek2, collapse = '-'), frmt)
                                    end <- as.numeric(end)
                                    daty <- paste(dek[1], dek[2], paste0('dek', dek[3]), sep = '-')
                                    list(date = daty, time = c(start, end))
                                }),
                        'monthly' = local({
                                    start <- as.Date(paste0(date, '-01'))
                                    end_date <- format(start, '%Y%m%d')
                                    nbday <- nb_day_of_month(end_date)
                                    end <- as.Date(paste0(date, '-', nbday))
                                    start <- as.numeric(start)
                                    end <- as.numeric(end)
                                    list(date = date, time = c(start, end))
                                })
                       )

    return(date_time)
}

aggregateTS_PenDekMon_Index <- function(dates, time_step){
    yymm <- format(dates, "%Y%m")
    if(time_step ==  "pentad"){
        jour <- as.numeric(format(dates, "%d"))
        jour <- cut(jour, c(1, 5, 10, 15, 20, 25, 31),
                    labels = FALSE, include.lowest = TRUE)
        index <- split(seq_along(dates), paste0(yymm, jour))
        nbday_fun <- nb_day_of_pentad

        odates <- as.Date(names(index), "%Y%m%d")
        sdates <- seq(min(odates), max(odates), 'day')
        tmp <- as.numeric(format(sdates, '%d'))
        ix <- tmp < 7
        it <- c(3, 7, 13, 17, 23, 27)[tmp[ix]]
        sdates <- as.Date(paste0(format(sdates, "%Y-%m-")[ix], it))

        pen <- as.numeric(format(odates, "%d"))
        pen <- c(3, 7, 13, 17, 23, 27)[pen]
        odates <- as.Date(paste0(format(odates, "%Y-%m-"), pen))
    }

    if(time_step ==  "dekadal"){
        jour <- as.numeric(format(dates, "%d"))
        jour <- cut(jour, c(1, 10, 20, 31),
                    labels = FALSE, include.lowest = TRUE)
        index <- split(seq_along(dates), paste0(yymm, jour))
        nbday_fun <- nb_day_of_dekad

        odates <- as.Date(names(index), "%Y%m%d")
        sdates <- seq(min(odates), max(odates), 'day')
        tmp <- as.numeric(format(sdates, '%d'))
        ix <- tmp < 4
        it <- c(5, 15, 25)[tmp[ix]]
        sdates <- as.Date(paste0(format(sdates, "%Y-%m-")[ix], it))

        dek <- as.numeric(format(odates, "%d"))
        dek <- c(5, 15, 25)[dek]
        odates <- as.Date(paste0(format(odates, "%Y-%m-"), dek))
    }

    if(time_step ==  "monthly"){
        index <- split(seq_along(dates), yymm)
        nbday_fun <- nb_day_of_month

        odates <- as.Date(paste(names(index), 15), "%Y%m%d")
        sdates <- seq(min(odates), max(odates), 'month')
    }

    pmon <- lapply(index, function(x) as.numeric(format(dates[x][1], "%m")))
    nbd0 <- sapply(seq_along(pmon), function(j) nbday_fun(names(pmon[j])))
    nobs <- sapply(index, length)

    out <- list(index = index, nobs = nobs, nobs0 = nbd0,
                sdates = sdates, odates = odates)
    return(out)
}


format_aggregate_dates <- function(dates, time_step){
    switch(time_step,
           "hourly" = format(dates, "%Y%m%d%H"),
           "daily" = format(dates, "%Y%m%d"),
           "pentad" = local({
                day <- as.numeric(format(dates, '%d'))
                brks <- c(1, 5, 10, 15, 20, 25, 31)
                pen <- cut(day, brks, labels = FALSE, include.lowest = TRUE)
                paste0(format(dates, "%Y%m"), pen)
           }),
           "dekadal" = local({
                day <- as.numeric(format(dates, '%d'))
                brks <- c(1, 10, 20, 31)
                dek <- cut(day, brks, labels = FALSE, include.lowest = TRUE)
                paste0(format(dates, "%Y%m"), dek)
           }),
           "monthly" = format(dates, "%Y%m"),
        )
}
