#' Load a CSV file into a distributed data frame.
#'
#' @section Partitioning between executors:
#'
#' We generate as many partitions as files. If there are less files than executors we split each file further.
#'
#' E.g. let’s say we have 3 executors and we try to load /tmp/*.csv which expands to [/tmp/file1.csv (500MB) and /tmp/file2.csv (1MB)]. Initially we create 2 partitions (the number of files). As we have more executors (3) than partitions (2) we further divide the biggest file into 2. In the end we have 3 partitions (/tmp/file1.csv from 0 to 250MB, /tmp/file1.csv from 250MB to 500MB and /tmp/file2.csv.
#'
#' If globbing is not used we only load one file which will be divided in as many chunks as executors.
#'
#' @section Details:
#'
#' There is a limitation in the case where the number of lines is less than the number executors.
#' In this case the load will fail. R's function \code{read.csv()} can be used instead.
#'
#' @param url File URL. Examples: '/tmp/file.csv', 'hdfs:///file.csv'.
#'
#'                      We also support globbing. Examples: '/tmp/*.csv', 'hdfs:///tmp/*.csv'.
#'
#'                      When globbing all CSV files need to have the same schema and delimiter.
#' @param schema  Specifies the column names and types.
#'
#'                Syntax is: \code{<col0-name>:<col0_type>,<col1-name>:<col1_type>,...<colN-name>:<colN_type>}.
#'
#'                Supported types are: \code{logical}, \code{integer}, \code{int64}, \code{numeric} and \code{character}. 
#'
#'                Example: schema='age:int64,name:character'.
#'
#'                Note that due to R not having a proper int64 type we convert it to an R numeric. Type conversion work as follows:
#'
#'                \tabular{ll}{
#'                    CSV type  |\tab R type    \cr
#'                    -         |\tab -         \cr
#'                    integer   |\tab integer   \cr
#'                    numeric   |\tab numeric   \cr
#'                    logical   |\tab logical   \cr
#'                    int64     |\tab numeric   \cr
#'                    character |\tab character
#'                }
#' @param delimiter Column separator. Example: delimiter='|'. By default delimiter is ','.
#' @param commentCharacter Discard lines starting with this character. Leading spaces are ignored.
#' @param hdfsConfigurationFile By default: \code{paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')}.
#'
#'                              Options are:
#'                              \itemize{
#'                                  \item webhdfsPort: webhdfs port, integer
#'                                  \item hdfsPort: hdfs namenode port, integer
#'                                  \item hdfsHost: hdfs namenode host, string
#'                                  \item hdfsUser: hdfs username, string
#'                              }
#'
#'                              An example file is:
#'
#'                                  \{ \cr
#'                                  "webhdfsPort": 50070, \cr
#'                                  "hdfsPort": 9000, \cr
#'                                  "hdfsHost": "172.17.0.3", \cr
#'                                  "hdfsUser": "jorgem" \cr
#'                                  \}
#'
#' @param skipHeader Treat first line as the CSV header and discard it.
#' @return A distributed data frame representing the CSV file.
#' @examples
#' df <- csv2dframe(url=paste(system.file(package='HPdata'),'/tests/data/ex001.csv',sep=''), schema='a:int64,b:character')

csv2dframe <- function(url, schema, delimiter=',', commentCharacter='#', 
                       hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep=''),
                       skipHeader=FALSE) {
    options = list()
    options['schema'] = schema
    options['delimiter'] = delimiter
    options['commentCharacter'] = commentCharacter
    options['hdfsConfigurationFile'] = hdfsConfigurationFile
    options['skipHeader'] = skipHeader

    options['fileType'] = 'csv'
    .ddc_read(url, options)
#    tryCatch({
#        .ddc_read(url, options)
#    }, error = function(e){
#        if (grepl('attempt to set partition',paste(e)) == TRUE) {
#            # retry with read.csv
#            warning('CSV file has less lines than executors. Trying with read.csv ...')
#            if (grepl('hdfs://',url) == TRUE) {
#                stop('Unable to read hdfs files with read.csv. Try starting Distributed R with only one executor (inst=1).')
#            }
#            d <- dframe(npartitions=c(1,1))
#            foreach(i,
#                1:npartitions(d),
#                func <- function(dhs = splits(d,i),
#                                 url = url) {
#                    dhs <- read.csv(url)
#                    update(dhs)
#            })
#        }
#        else {
#            stop(e)
#        }
#    })
}

#' Load an ORC file into a distributed data frame
#'
#' @section Partitioning between executors:
#'
#' We generate as many partitions as the total number of ORC stripes.
#'
#' E.g. let’s say the customer tries to load /tmp/*.orc which expands to /tmp/file1.orc with 2 stripes and /tmp/file2.orc with 3 stripes. We’ll end up with 2 + 3 = 5 partitions.
#'
#' If globbing is not used we only load one file and the number of partitions will be equal to the number of stripes.
#'
#' @section Details:
#'
#' Type conversions work as follows:
#'                \tabular{ll}{
#'                    ORC type          |\tab R type        \cr
#'                    -                 |\tab -             \cr
#'                    byte/short/int    |\tab integer       \cr
#'                    float/double      |\tab numeric       \cr
#'                    long              |\tab numeric       \cr
#'                    bool              |\tab logical       \cr
#'                    string/binary     |\tab character     \cr
#'                    char/varchar      |\tab character     \cr
#'                    decimal           |\tab character     \cr
#'                    timestamp/date    |\tab character     \cr
#'                    union             |\tab not supported \cr
#'                    struct            |\tab dataframe     \cr
#'                    map               |\tab dataframe     \cr
#'                    list              |\tab list
#'                }
#'
#' @param url File URL. Examples: '/tmp/file.orc', 'hdfs:///file.orc'.
#' @param selectedStripes ORC stripes to include. Stripes need to be consecutive. If not specified defaults to all the stripes in the ORC file.
#' @param hdfsConfigurationFile By default: \code{paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')}.
#'
#'                              Options are:
#'                              \itemize{
#'                                  \item webhdfsPort: webhdfs port, integer
#'                                  \item hdfsPort: hdfs namenode port, integer
#'                                  \item hdfsHost: hdfs namenode host, string
#'                                  \item hdfsUser: hdfs username, string
#'                              }
#'
#'                              An example file is:
#'
#'                                  \{ \cr
#'                                  "webhdfsPort": 50070, \cr
#'                                  "hdfsPort": 9000, \cr
#'                                  "hdfsHost": "172.17.0.3", \cr
#'                                  "hdfsUser": "jorgem" \cr
#'                                  \}
#' @return A distributed data frame representing the ORC file.
#' @examples
#' df <- orc2dframe(url=paste(system.file(package='HPdata'),'/tests/data/TestOrcFile.test1.orc',sep=''))


orc2dframe <- function(url, selectedStripes='', 
                       hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')) {
    options = list()
    options['selectedStripes'] = selectedStripes
    options['hdfsConfigurationFile'] = hdfsConfigurationFile

    options['fileType'] = 'orc'
    .ddc_read(url, options)
}

.ddc_read <- function(url, options) {
    if(!("hdfsConfigurationFile" %in% names(options))) {
        # set default hdfsConfigurationFile
        options["hdfsConfigurationFile"] = paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')
    }


    pm <- get_pm_object()
    # 1. Schedule file across workers. Handles globbing also.
    library(hdfsconnector)
    plan <- hdfsconnector::create_plan(url, options, pm$worker_map())

    hdfsConfigurationStr <- paste(readLines(as.character(options["hdfsConfigurationFile"])),collapse='\n')
    for (i in 1:length(plan$configs)) {
        plan$configs[[i]]["hdfsConfigurationStr"] = hdfsConfigurationStr
        if("skipHeader" %in% names(options)) {
            plan$configs[[i]]["skipHeader"] = as.logical(options["skipHeader"])
        }
    }

    if (Sys.getenv('DEBUG_DDC') != '') {
        print(plan)  # for debugging
    }

    # set chunk_worker_map in master so dframe partitions are created on the right workers
    pm$ddc_set_chunk_worker_map(plan$chunk_worker_map)

    #
    # plan$num_partitions
    #
    # plan$configs
    #   configs is an array of configurations, one for each worker. Each config has all
    #   the information the worker needs to featch a split

    # 2. Create darray
    # When using the "ddc" policy each split is assigned to a worker according to the plan
    # generated in pm$ddc_schedule
    d <- dframe(npartitions=c(plan$num_partitions,1), distribution_policy='ddc')

    # 3. Load each split on the workers
    foreach(i, 
            1:npartitions(d), 
            func <- function(dhs = splits(d,i),
                             url = url,
                             config = plan$configs[[i]]) {
                library(hdfsconnector)
                if (config$file_type == "csv") {
                    dhs <- csv2dataframe(config$url,
                                         schema=config$schema,
                                         chunkStart=config$chunk_start,
                                         chunkEnd=config$chunk_end,
                                         delimiter=config$delimiter,
                                         commentCharacter=config$comment_character,
                                         hdfsConfigurationStr=config$hdfsConfigurationStr,
                                         skipHeader=config$skipHeader)
                    update(dhs)
                }
                else if (config$file_type == "orc") {
                    dhs <- orc2dataframe(url,
                                         selectedStripes=config$selected_stripes,
                                         hdfsConfigurationStr=config$hdfsConfigurationStr)
                    update(dhs)
                }
                else {
                    stop("Unsupported file type")
                }
            }
    )

    c <- NULL
    if("schema" %in% names(options)) {
        # CSV
        c <- hdfsconnector::schema2colnames(as.character(options['schema']))
    }
    else if(as.character(options['fileType']) == 'orc') {
        # ORC
        c <- hdfsconnector::orccolnames(url, as.character(options['hdfsConfigurationFile']))
    }
    colnames(d) <- c;
    d # return dframe
}
