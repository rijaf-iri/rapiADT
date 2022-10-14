#' Plot wind barb.
#'
#' Get wind data to display the wind barb chart.
#'
#' @param user_req a JSON object containing the user request.
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @return JSON object
#' 
#' @export

chartWindBarb <- function(user_req, aws_dir){
    user_req <- jsonlite::parse_json(user_req)
    # saveRDS(user_req, file = '/Users/rijaf/Desktop/json_args.rds')
    # user_req <- readRDS('/Users/rijaf/Desktop/json_args.rds')

    out <- list(opts = list(status = 'Test wind barb'))
    return(convJSON(out))
}
