# GDELT Analysis Project
This project attempts to answer a research question using data from [GDELT 2.0’s `events` database](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/), by analysing worldwide news coverage across 15 minute intervals.

### Research Question
How similar is the *prevalence of events reporting meetings*, when compared with equivalent prevalences across the preceding year?

It is hypothesized is that any difference, if observed, is likely attributable to COVID-19.

### Results
The similarity in the prevalence of meetings is the same between May 2020 and May 2019, concluding that there is no difference in the prevalence of meetings as a result of COVID-19, across news reports from New Zealand, Australia, Canada, the U.S. and the UK. (99.2% similarity in prevalence)

The output from the analysis is viewable [here](analysis_results.txt).

## Installation and Execution
Dependencies are listed within `requirements.txt`, so simply run (you may want to create a new virtualenv first):

```bash
$ pip install -r requirements.txt
```

With Apache Spark installed you can then perform the analysis:

```bash
$ python3 process.py
```

This will attempt to download cached GDELT data (~4 GB) from my GCP bucket; it will also download data directly from GDELT for each date reported by the analysis configuration that isn’t cached. Downloading everything (~360 days, ~55M rows with the default config) directly from GDELT will take a very long time, on the order of a few hours. See [this list](cached.md) for all event dates which are stored in the cache.

Once the cached event files have downloaded, the extraction is delegated to `tar` called from within `download.py`. Therefore firstly ensure that `tar` is present within your `PATH`. As this is a largely single-threaded task, this can take some time. **The final expanded archive is around 25 GB in size!**

### Note
This is likely a very inefficient algorithm. It can be parallelized across approximately 16 concurrent workers before performance gains are negligible. By default all CPU threads will be allocated to Spark; expect a 4-core hyperthreaded Core i7 to finish analysis in around 10–15 minutes—see results [here](scalability.pdf) for in-depth concurrency analysis.

The implementation also can be sped up by allocating more memory to Spark, this can be modified on [line 14](https://github.com/Senyeah/data301-project/blob/master/process.py#L14) in `process.py`:
```python
# Configuration I used locally
conf = SparkConf().setMaster('local[*]').set('spark.executor.memory', '16g').set('spark.driver.memory', '16g')
```
I have observed memory usage [in excess of 130 GB](https://i.imgur.com/J8b21mu.png) when such a large amount is allocated, so beware!

## Terminology
This project allows for analysis to occur across a variable period of dates. To facilitate this, several definitions allow for the modification what dates are analysed.

### Time slices
