#' Get all available variables.
#'
#' Get all available variables for AWS Data Status.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

getStatusVariables <- function(aws_dir){
    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS") 
    fileStatus <- file.path(dirSTATUS, "aws_variables.rds")
    vars <- readRDS(fileStatus)

    return(convJSON(vars))
}

#############
#' Get AWS Status data.
#'
#' Get AWS Status data to display on map.
#' 
#' @param ltime character, the last time duration to display. Options are, "01h", "03h", "06h", 
#' "12h", "24h", "02d", "03d", "05d", "01w", "02w", "03w", "01m".
#' @param varh character, code variable and height: "codevar_height".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return a JSON object
#' 
#' @export

getStatusData <- function(ltime, varh, aws_dir){
    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS") 
    filestatus <- file.path(dirSTATUS, paste0("aws_status_", varh, ".rds"))
    aws <- readRDS(filestatus)
    vtime <- as.numeric(substr(ltime, 1, 2))
    ttime <- substr(ltime, 3, 3)
    hmul <- switch(ttime, "h" = 1, "d" = 24, "w" = 168, "m" = 720)
    hour <- vtime * hmul
    if(hour > 1){
        ic <- (720 - hour + 1):720
        stat <- aws$status[, ic]
        stat <- rowMeans(stat, na.rm = TRUE)
    }else{
        nc <- ncol(aws$status)
        stat <- aws$status[, nc]
    }

    don <- aws$coords
    don$data <- round(stat, 1)

    ##########
    breaks <- seq(10, 90, 10)
    userOp <- list(
                   # pColors = list(color = 'rainbow', reverse = FALSE),
                   uColors = list(custom = TRUE, color = grDevices::rainbow(10, end = 0.7)),
                   levels = list(custom = TRUE, levels = breaks, equidist = TRUE)
                  )
    pars <- image.plot_Legend_pars(NULL, userOp)
    ckey <- list(labels = pars$legend.axis$labels, colors = pars$colors)

    zmin <- suppressWarnings(min(don$data, na.rm = TRUE))
    if(!is.infinite(zmin)){
        pars$breaks[1] <- ifelse(pars$breaks[1] > zmin, zmin, pars$breaks[1])
    }
    zmax <- suppressWarnings(max(don$data, na.rm = TRUE))
    if(!is.infinite(zmax)){
        nl <- length(pars$breaks)
        pars$breaks[nl] <- ifelse(pars$breaks[nl] < zmax, zmax, pars$breaks[nl])
    }

    kolor <- pars$colors[findInterval(don$data, pars$breaks, rightmost.closed = TRUE, left.open = TRUE)]

    ## zero set to white
    ix <- !is.na(don$data) & don$data == 0
    kolor[ix] <- "#FFFFFF"

    don <- list(data = don, color = kolor, key = ckey,
                time = format(aws$actual_time, "%Y-%m-%d %H:%M:%S"),
                update = format(aws$updated, "%Y-%m-%d %H:%M:%S"))

    return(convJSON(don))
}

##########
#' Download hourly AWS status.
#'
#' Download hourly AWS status table, no user request.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSStatusNoUserReq <- function(aws_dir){
    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS")
    file_status <- file.path(dirSTATUS, "aws_status_0_0.rds")
    aws_status <- readRDS(file_status)

    crds <- aws_status$coords
    daty <- format(aws_status$time, "%Y-%m-%d %H:00:00")
    don <- as.data.frame(aws_status$status)
    names(don) <- daty
    don <- cbind(crds, don)

    # format filename
    filename <- paste0("AWS_Status_All_data_",
                       format(aws_status$actual_time, "%Y-%m-%d-%H"),
                       ".csv")
    out <- list(filename = filename, csv_data = convCSV(don))

    return(convJSON(out))
}

##########
#' Download hourly AWS status.
#'
#' Download hourly AWS status table, with user request, use GET.
#' 
#' @param varh character, code variable and height: "codevar_height".
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSStatusUserReqGET <- function(varh, aws_dir){
    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS") 
    var_status <- readRDS(file.path(dirSTATUS, 'aws_variables.rds'))
    file_status <- file.path(dirSTATUS, paste0("aws_status_", varh, ".rds"))
    aws_status <- readRDS(file_status)

    crds <- aws_status$coords
    daty <- format(aws_status$time, "%Y-%m-%d %H:00:00")
    don <- as.data.frame(aws_status$status)
    names(don) <- daty
    don <- cbind(crds, don)

    vh <- var_status[var_status$value %in% varh, ]
    vn <- gsub(' ', '-', vh$name)
    h <- paste0(vh$height, 'm')
    st <- format(aws_status$actual_time, "%Y-%m-%d-%H")
    filename <- paste0(vn, '_', h, '_', st, '.csv')

    out <- list(filename = filename, csv_data = convCSV(don))

    return(convJSON(out))
}

##########
#' Download hourly AWS status.
#'
#' Download hourly AWS status table, with user request, use POST.
#' 
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

downAWSStatusUserReqPOST <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    varh <- user_req$var_hgt
    user_info <- user_req$user

    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS") 
    var_status <- readRDS(file.path(dirSTATUS, 'aws_variables.rds'))
    file_status <- file.path(dirSTATUS, paste0("aws_status_", varh, ".rds"))
    aws_status <- readRDS(file_status)

    crds <- aws_status$coords
    daty <- format(aws_status$time, "%Y-%m-%d %H:00:00")
    don <- as.data.frame(aws_status$status)
    names(don) <- daty
    don <- cbind(crds, don)

    vh <- var_status[var_status$value %in% varh, ]
    vn <- gsub(' ', '-', vh$name)
    h <- paste0(vh$height, 'm')
    st <- format(aws_status$actual_time, "%Y-%m-%d-%H")
    filename <- paste0(vn, '_', h, '_', st, '.csv')

    out <- list(filename = filename, csv_data = convCSV(don))

    return(convJSON(out))
}

