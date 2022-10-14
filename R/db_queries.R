
create_query_and <- function(query_args){
    args <- lapply(seq_along(query_args), function(j){
        nom <- names(query_args[j])
        val <- query_args[[j]]
        if(nom == 'id'){
            if(length(val) == 1){
                qr <- paste0(nom, "=", "'", val, "'")
            }else{
                qr <- paste0("'", val, "'", collapse = ", ")
                qr <- paste0(nom, " IN (", qr, ")")
            }
        }else{
            if(length(val) == 1){
                qr <- paste0(nom, "=", val)
            }else{
                qr <- paste0(val, collapse = ", ")
                qr <- paste0(nom, " IN (", qr, ")")
            }
        }

        qr
    })
    args <- do.call(c, args)
    query <- paste(args, collapse = " AND ")
    query <- paste0("(", query, ")")
    query
}

create_query_times <- function(colname_time, start_time, end_time = NULL,
                                   opr1 = ">=", opr2 = "<=")
{
    if(is.null(end_time)){
        if(length(start_time) == 1){
            qr <- paste(colname_time, opr1, start_time)
        }else{
            qr <- paste0(start_time, collapse = ", ")
            qr <- paste0("(", qr, ")")
            qr <- paste(colname_time, "IN", qr)
        }
    }else{
        start_time <- start_time[1]
        end_time <- end_time[1]
        qr <- paste(colname_time, opr1, start_time, "AND", colname_time, opr2, end_time)
    }

    query <- paste0("(", qr, ")")
    query
}

create_query_select <- function(table_name, cols_select = "*",
                                query_args = NULL, query_time = NULL)
{
    cols <- paste0(cols_select, collapse = ', ')
    select <- paste("SELECT", cols, "FROM", table_name)

    query1 <- NULL
    if(!is.null(query_args))
        query1 <- create_query_and(query_args)

    query2 <- NULL
    if(!is.null(query_time))
        query2 <- do.call(create_query_times, query_time)

    if(!is.null(query1) & !is.null(query2)){
        query <- paste(select, "WHERE", query1, "AND", query2)
    }else if(!is.null(query1) & is.null(query2)){
        query <- paste(select, "WHERE", query1)
    }else if(is.null(query1) & !is.null(query2)){
        query <- paste(select, "WHERE", query2)
    }else{
        query <- NULL
    }

    return(query)
}

create_query_select_ptime_discrete <- function(conn, table_name, query_time,
                                               query_args = NULL, cols_select = "*")
{
    partition_years <- as.numeric(format(query_time$start_time, '%Y'))
    pexist <- check_db_partition_years(conn, table_name, partition_years)
    if(!any(pexist)) return(NULL)

    partition_name <- paste0('p', partition_years[pexist])
    tempsN <- as.numeric(query_time$start_time[pexist])

    pindex <- split(seq_along(partition_name), partition_name)
    query2 <- lapply(seq_along(pindex), function(j){
        qt <- query_time
        qt$start_time <- tempsN[pindex[[j]]]
        if(length(pindex[[j]]) == 1) qt$opr1 <- "="
        qr <- do.call(create_query_times, qt)
        list(p = names(pindex[j]), q = qr)
    })

    cols <- paste0(cols_select, collapse = ', ')
    select <- paste("SELECT", cols, "FROM", table_name)

    query1 <- NULL
    if(!is.null(query_args))
        query1 <- create_query_and(query_args)

    query <- lapply(query2, function(qr){
        partn <- paste0("PARTITION (", qr$p, ")")
        if(!is.null(query1)){
            paste(select, partn, "WHERE", query1, "AND", qr$q)
        }else{
            paste(select, partn, "WHERE", qr$q)
        }
    })

    return(query)
}

create_query_select_ptime_range <- function(conn, table_name, query_time,
                                            query_args = NULL, cols_select = "*",
                                            tz = Sys.getenv("TZ"))
{
    yr1 <- as.integer(format(query_time$start_time, '%Y'))
    yr2 <- as.integer(format(query_time$end_time, '%Y'))
    pyear <- yr1:yr2

    pthres <- lapply(pyear, function(y){
        y1 <- paste0(y, "-01-01 00:00")
        y2 <- paste0(y + 1, "-01-01 00:00")
        y1 <- strptime(y1, "%Y-%m-%d %H:%M", tz = tz)
        y2 <- strptime(y2, "%Y-%m-%d %H:%M", tz = tz)
        y2 <- y2 - 1

        c(y1, y2)
    })
    pthres[[1]][1] <- query_time$start_time
    pthres[[length(pyear)]][2] <- query_time$end_time
    pthres <- lapply(pthres, as.numeric)

    pexist <- check_db_partition_years(conn, table_name, pyear)
    if(!any(pexist)) return(NULL)

    pyear <- pyear[pexist]
    pthres <- pthres[pexist]

    query2 <- lapply(seq_along(pyear), function(j){
        qt <- query_time
        qt$start_time <- pthres[[j]][1]
        qt$end_time <- pthres[[j]][2]
        qr <- do.call(create_query_times, qt)
        list(p = paste0('p', pyear[j]), q = qr)
    })

    cols <- paste0(cols_select, collapse = ', ')
    select <- paste("SELECT", cols, "FROM", table_name)

    query1 <- NULL
    if(!is.null(query_args))
        query1 <- create_query_and(query_args)

    query <- lapply(query2, function(qr){
        partn <- paste0("PARTITION (", qr$p, ")")
        if(!is.null(query1)){
            paste(select, partn, "WHERE", query1, "AND", qr$q)
        }else{
            paste(select, partn, "WHERE", qr$q)
        }
    })

    return(query)
}
