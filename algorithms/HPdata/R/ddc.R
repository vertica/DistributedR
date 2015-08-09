csv2dframe <- function(url, ...) {
    options = list(...)
    options['fileType'] = 'csv'
    .ddc_read(url, options)
}

orc2dframe <- function(url, ...) {
    options = list(...)
    options['fileType'] = 'orc'
    .ddc_read(url, options)
}

.ddc_read <- function(url, options) {
    if(!("hdfsConfigurationFile" %in% options)) {
        # set default hdfsConfigurationFile
        options["hdfsConfigurationFile"] = paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')
    }
    pm <- get_pm_object()
    # 1. Schedule file across workers. Handles globbing also.
    library(hdfsconnector)
    plan <- create_plan(url, options, pm$worker_map())
    #print(plan)  # for debugging

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
                                         hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),
                                                                     '/conf/hdfs.json',
                                                                     sep=''))
                    update(dhs)
                }
                else if (config$file_type == "orc") {
                    dhs <- orc2dataframe(url,
                                         selectedStripes=config$selected_stripes,
                                         hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),
                                                                     '/conf/hdfs.json',
                                                                     sep=''))
                    update(dhs)
                }
                else {
                    stop("Unsupported file type")
                }
            }
    )
    d # return dframe
}
