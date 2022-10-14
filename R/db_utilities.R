
connect.DBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.RODBC <- function(con_args){
    args <- paste0(names(con_args), '=', unlist(con_args))
    args <- paste(args, collapse = ";")
    args <- list(connection = args, readOnlyOptimize = TRUE)
    con <- try(do.call(RODBC::odbcDriverConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.adt_db <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.con")
    adt <- readRDS(ff)
    conn <- connect.DBI(adt$connection, RMySQL::MySQL())
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adt$connection, RMySQL::MySQL())
        if(is.null(conn)) return(NULL)
    }

    DBI::dbExecute(conn, "SET GLOBAL local_infile=1")
    return(conn)
}

check_db_partition_years <- function(conn, table_name, partition_years){
    query <- paste0("SELECT PARTITION_NAME FROM information_schema.partitions WHERE table_name='", table_name, "'")
    pinfo <- DBI::dbGetQuery(conn, query)
    p_years <- as.integer(substr(pinfo$PARTITION_NAME, 2, 5))
    partition_years %in% p_years
}
