
convJSON <- function(obj, ...){
    args <- list(...)
    if(!'pretty' %in% names(args)) args$pretty <- TRUE
    if(!'auto_unbox' %in% names(args)) args$auto_unbox <- TRUE
    if(!'na' %in% names(args)) args$na <- "null"
    args <- c(list(x = obj), args)
    json <- do.call(jsonlite::toJSON, args)
    return(json)
}

convCSV <- function(obj, col.names = TRUE){
    filename <- tempfile()
    utils::write.table(obj, filename, sep = ",", na = "", col.names = col.names,
                row.names = FALSE, quote = FALSE)
    don <- readLines(filename)
    unlink(filename)
    don <- paste0(don, collapse = "\n")

    return(don)
}

orderedVariables <- function(v_vect, v_order){
    var_tab <- unique(v_vect)
    var_tab <- var_tab[!var_tab %in% v_order]
    var_order <- c(v_order, var_tab)
    var_order <- order(match(v_vect, var_order))
    return(var_order)
}
