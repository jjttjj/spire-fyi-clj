# spire-fyi-clj

A Clojure application to support the data collection for [spire.fyi](https://spire.fyi).

* Schedules, executes, and stores query results from the [Flipside crypto api](https://docs.flipsidecrypto.com/shroomdk-sdk/get-started/rest-api). Integration with additional data sources will be added in the future.
* Uses [Datalevin](https://github.com/juji-io/datalevin) for storage: its datalog api for query/job metadata and the lmbd wrapper to store timeseries/associative data.
* Result data is uploaded to S3 for usage by the spire.fyi frontend.
