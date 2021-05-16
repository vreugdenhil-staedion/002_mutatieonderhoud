# FUNCTIONS -------------------------------------------------------------

#' Package loader
#'
#' This function loads R packages into your work environment.
#' When a package is not yet installed on your computer, it will be downloaded and installed.
#'
#' @param pkg A vector with one or more package names.
#'
#' @examples
#' fn.packages(c("tidyverse"))
#' fn.packages(c("tidyverse", "knitr"))
#'
fn.packages <- function(pkg){
  print("Install and load multiple R packages")
  print(pkg)

  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    utils::install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}


#' Get data from Microsoft SQL Server database
#'
#' Establishes a connection to a Microsoft SQL Server database, executes a query
#' and returns the data collected by the query.
#'
#' @param server Microsoft SQL Server (host name or IP address)
#' @param database SQL database name
#' @param query SQL query
#'
#' @return Data set collected by SQL query
#'
#' @examples
#' fn.mssql("hostname", "database", "select * from table")
#' fn.mssql("sqlserver2019", "empire", "select * from customer")
#'
fn.mssql <- function(server, database, query) {
  dbhandle <- RODBC::odbcDriverConnect(paste0("driver={SQL Server};server=", server, ";database=", database, ";trusted_connection=true"))
  print(paste0("Connected to Microsoft SQL Server database ", server, ".", database))

  print("Execute query")
  data <- RODBC::sqlQuery(dbhandle, query)
  print(paste0("Retrieved ", formatC(nrow(data), big.mark=","), " records"))

  RODBC::odbcClose(dbhandle)
  print("Closed connection to database")

  return(data)
}


#' Get query string from sql file
#'
#' Converts a .sql file to a proper query string.
#' It takes care of tab and comments.
#'
#' @param filepath Location of .sql file
#'
#' @return Vector with query string
#'
#' @examples
#' fn.qry2string("src/myquery.sql")
#'
fn.qry2string <- function(filepath){
  print(paste0("Get query from sql file ", filepath))
  con = file(filepath, "r")
  sql.string <- ""

  while (TRUE){
    line <- readLines(con, n = 1)

    if ( length(line) == 0 ){
      break
    }

    line <- gsub("\\t", " ", line)

    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }

    sql.string <- paste(sql.string, line)
  }

  close(con)
  return(sql.string)
}


#' Import data frame to SQLite database
#'
#' @param dataframe Data frame: Source data stored in a data frame
#' @param database SQL: SQLite database connection name
#' @param table SQL: Target SQLite table name
#' @param index SQL: Field name(s) to create an index for
#'
#' @examples
#' fn.df2sql(mydf, sqllite_connection, "mysqltable", "id")
#' fn.df2sql(mydf, sqllite_connection, "mysqltable", c("id1","id2"))
#'
fn.df2sql <- function(dataframe, database, table, index) {
  print(paste0("Import data frame to SQLite database ", deparse(substitute(database)), ".", table))

  # Drop SQLite table if exists
  if(DBI::dbExistsTable(database, table)) {
    DBI::dbRemoveTable(database, table)
    print(paste0("Dropped table ", deparse(substitute(database)), ".", table))
  }

  # Create SQLite table
  DBI::dbCreateTable(database, table, dataframe)
  print(paste0("Created table ", deparse(substitute(database)), ".", table))

  # Copy data from df to SQL
  dplyr::copy_to(database, dataframe, table,
                 temporary=FALSE,
                 overwrite=TRUE,
                 indexes=list(index)
  )

  # Print results
  df.cnt <- nrow(dataframe) %>% as.integer()
  db.cnt <- DBI::dbGetQuery(sql.db, paste0("select count(*) from ", table)) %>% as.integer()

  print(paste0("Copied ", formatC(db.cnt, big.mark=",") , " of ", formatC(df.cnt, big.mark=","), " records from data frame ", deparse(substitute(dataframe)), " to table ", deparse(substitute(database)), ".", table, ". Index set on column(s) ", str_c(unique(index), collapse=", ")))
}


#' Import CSV file to SQLite database
#'
#'
#' @param file CSV: Filepath to CSV file with source data
#' @param sep CSV: Field seperator used in CSV file
#' @param encoding CSV: Encoding used in CSV file (default: UTF-8)
#' @param database SQL: SQLite database connection name
#' @param table SQL: Target SQLite table name
#' @param index SQL: Field name(s) to create an index for in the SQLite database
#' @examples
#' fn.csv2sql("raw_data/mycsv.csv", ";", "UTF-8", sqllite_connection, "mysqltable", "id")
#' fn.csv2sql("raw_data/mycsv.csv", ";", "UTF-8", sqllite_connection, "mysqltable", c("id1","id2"))
#'
fn.csv2sql <- function(file, sep, encoding="UTF-8", database, table, index) {
  print(paste0("Import CSV file ", file, " to SQLite database ", deparse(substitute(database)), ".", table))

  # Drop SQLite table if exists
  if(DBI::dbExistsTable(database, table)) {
    DBI::dbRemoveTable(database, table)
    print(paste0("Dropped table ", deparse(substitute(database)), ".", table))
  }

  # Create SQLite table
  DBI::dbCreateTable(database, table, dataframe)
  print(paste0("Created table ", deparse(substitute(database)), ".", table))

  # Copy data from CSV to SQL
  dplyr::copy_to(database, read.csv(file, header=TRUE, sep=sep, fileEncoding=encoding), table,
                 temporary=FALSE,
                 overwrite=TRUE,
                 indexes=list(index)
  )

  # Print results
  csv.cnt <- nrow(read.csv(file, header=TRUE, sep=sep, fileEncoding=encoding)) %>% as.integer()
  db.cnt <- DBI::dbGetQuery(sql.db, paste0("select count(*) from ", table)) %>% as.integer()

  print(paste0("Copied ", formatC(db.cnt, big.mark=",") , " of ", formatC(csv.cnt, big.mark=","), " records from CSV file ", file, " to table ", deparse(substitute(database)), ".", table, ". Index set on column(s) ", str_c(unique(index), collapse=", ")))
}


#' Get structure of SQLite table
#'
#' @param database SQLite database connection name
#' @param table SQLite table name
#'
#' @return SQLite table structure with field name(s) and data type returned as data frame
#'
#' @examples
#' fn.sqlstructure(sqllite_connection, "mysqltable")
#'
fn.sqlstructure <- function(database, table) {
  sqlstructure <-  dplyr::tbl(database, table) %>%
    utils::head() %>%
    dplyr::collect() %>%
    lapply(class) %>%
    unlist() %>%
    as.data.frame()

  colnames(sqlstructure) <- "Data type"
  sqlstructure <- cbind("SQLite field name"=rownames(sqlstructure), sqlstructure)
  rownames(sqlstructure) <- NULL

  return(sqlstructure)
}
