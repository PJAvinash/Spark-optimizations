# Optimized way to union an array of Dataframes
**Usecases**
- reading mutiple .csv/.parquet files generated by different sources and performing batch processing in later stages

**important details**
- this technique only discusses what should be done if we do not have an intention to use .localcheckpoint() 
- using .localcheckpoint() will shorten the logical plan, it will have slightly different performance characteristics 
