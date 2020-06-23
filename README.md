# spark-analytics

- [Task](#task)
- [Setup](#setup)
- [Optimisations](#optimisations)
- [Individual Contributions](#individual-contributions)

## Task

DATA3404 assignment: Implement an analytics pipeline using Spark's RDD and DataFrame API and improve performance with tuning.

Two of the tasks required joins on particular attributes. One of the tasks required filtering by date range and the other required ranking using the `WINDOW` function.

## Setup

We used AWS EMR (ElasticMap Reduce) service to run our Spark jobs on a cluster. For all tasks, we measured performance on `m.5xlarge` and later `m.4large`, when the larger VM was no longer available.

## Optimisations

I briefly explain choices for the optimisations we chose.

__1) Broadcast join__

We typically had a smaller dataset that would fit entirely in memory. Hence we specify a hint for the strategy we would like Spark to consider. In this case, `broadcast_join` was applicable.

Spark's latest verion 3.0.0 there is more information regarding performance tuning. At the time of completion, the Spark version used was 2.4.

__2) Offline re-partitioning__

We take advantage of knowing in advance the feature that would be used to join tables and conduct an offline re-partitioning before running the Spark job.

In task 2, we are required to filter the dataset by `country`. The baseline approach involves reading the entire dataset into memory and filtering during the Spark job.

To optimise performance, we first repartition the dataset by `country`, setting `spark.sql.files.maxRecordsPerFile` to a reasonable in order to prevent skewed partition sizes. Then during query-time, the Spark job will read only the partition of the data that is relevant to the query.

This was determined by observing the distribution of flights segmented by `country`.

This way, we reduce the reads on average by 80% (depending on `country` chosen) and increase the run-time performance on average by 100%.

__3) Allocating cluster resources__

The `m4.xarge` has `4vCore` and `8GB` of memory. The hardware configurations is set at 2 worker nodes and 1 master node (the driver).

Let's calculate the allocations.

Since we have two worker nodes, we have a total of `8GB` of memory. Taking `1GB` away for overhead, we are left with `7GB` of memory for each worker node.

Each worker node will hold `executors` which then have `executor-cores` that specify how many tasks can be run in parallel in a single executor.

Let's specify 2 `executors` per node, except for the driver node which will have 1 `executor`.

We also allocate some memory overhead for the jobs, ($\sim 7%$) and hence we allocate

$$3\text{GB} = \bigg\lceil \frac{7 - 0.07\times7}{2} \bigg\rceil$$

After some experimentation, the number of cores was chosen to be 4.

Hence we have

| Config | Value |
|:---:|:---:|
| # executors | 5 |
| # executor-cores | 4 |
| # executor-memory | 3GB |

## Individual Contributions

- Implemented the Spark jobs using DataFrame API in Python
- Applied offline re-partitioning optimisation to boost task 2 query performance by 100%
- Applied allocation of cluster resources to speed up all queries by 100%.

__I would like to acknowledge my teammates Gio and Niruth for implementing the RDD code and running various tests for performance tuning. The RDD API is not trivial to work with!__