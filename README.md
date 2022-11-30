This code must be run like this

```
sbt 'runMain akka.Main dyna.mapreddemo.name-of-example'
```

The expected output is
```
[info] Running akka.Main dyna.mapreddemo.name-of-example
[INFO] [07/23/2015 00:51:05.303] (...) INPUT CONSUMED - FINISHING PROCESSING
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 1 chunks
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 2 chunks
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 3 chunks
[INFO] [07/23/2015 00:51:05.310] (...) DONE: 4 chunks
[INFO] [07/23/2015 00:51:05.551] (...) REDUCER AGGREGATION COMPLETED
FINAL RESULTS
Map(e -> 2, s -> 4, n -> 3, t -> 3, a -> 4, m -> 2, i -> 5, b -> 1, l -> 1, h -> 1, r -> 1, d -> 1)
[INFO] [07/23/2015 00:51:05.558] (...) application supervisor has terminated, shutting down
[success] Total time: 30 s, completed 23/07/2015 00:51:05

```

The method `Mapred.props` receives as arguments the desired number of parallel workers, the mapper function to be applied to the input data, the reducer function to be applied to values corresponding to each key, and a final function that receives the aggregated data after the stream is completed.
# mapred
