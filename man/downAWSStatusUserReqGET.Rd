% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/aws_status_data.R
\name{downAWSStatusUserReqGET}
\alias{downAWSStatusUserReqGET}
\title{Download hourly AWS status.}
\usage{
downAWSStatusUserReqGET(varh, aws_dir)
}
\arguments{
\item{varh}{character, code variable and height: "codevar_height".}

\item{aws_dir}{full path to the directory containing the AWS_DATA folder.}
}
\value{
JSON object
}
\description{
Download hourly AWS status table, with user request, use GET.
}
