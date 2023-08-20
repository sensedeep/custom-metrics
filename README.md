
# CustomMetrics

![CustomMetrics](https://www.sensedeep.com/images/metrics.png)

## Low cost, fast, simple, scalable metrics for AWS

[![Build Status](https://img.shields.io/github/actions/workflow/status/sensedeep/custom-metrics/build.yml?branch=main)](https://img.shields.io/github/actions/workflow/status/sensedeep/custom-metrics/build.yml?branch=main)
[![npm](https://img.shields.io/npm/v/custom-metrics.svg)](https://www.npmjs.com/package/custom-metrics)
[![npm](https://img.shields.io/npm/l/custom-metrics.svg)](https://www.npmjs.com/package/custom-metrics)
[![Coverage Status](https://coveralls.io/repos/github/sensedeep/custom-metrics/badge.svg?branch=main)](https://coveralls.io/github/sensedeep/custom-metrics?branch=main)

CustomMetrics is a NodeJS library to emit and query custom metrics for AWS apps.

>CustomMetrics is under active development and is not (yet) ready for production use. This README documentation is incomplete. All feedback is welcome.

## Background

AWS CloudWatch offers metrics to monitor specific aspects of your apps that are not covered by the default AWS infrastructure metrics. 

Unfortunately, the AWS "custom" metrics can be very expensive. If updated or queried regularly. Each each custom metric will cost up to $5 per metric per year with additional costs for querying. If you have many metrics or high dimensionality on your metrics, this can lead to a very large CloudWatch Metrics bill.

> **CustomMetrics** provides cost effective metrics that are up to 1000 times cheaper and 10 times faster than standard CloudWatch metrics.

CustomMetrics achieves these savings by supporting only "latest" period metrics. i.e. last day, last month, last hour etc. This enables each metric to be saved, stored and queried with minimal cost.

## CustomMetrics Features

-   Simple one line API to emit metrics from any NodeJS TypeScript or JavaScript app.
-   Similar metric model to AWS supporting namespaces, metrics, dimensions, statistics and intervals.
-   Computes statistics for: average, min, max, count and sum.
-   Computes P value statistics with configurable P value resolution.
-   Supports a default metric intervals of: last 5 mins, hour, day, week, month and year.
-   Configurable custom intervals for higher or different metric intervals.
-   Fast and flexible query API to fetch by namespace, metric and dimensions.
-   Query API can return data points or aggregate metric as a single statistic.
-   Scalable to support many simultaneous clients emitting metrics.
-   Stores data in any existing DynamoDB table and can co-exist with existing app data.
-   Supports multiple services, apps, namespaces and metrics in a single DynamoDB table.
-   Extremely fast initialization time.
-   Written in TypeScript with full TypeScript support.
-   Clean, readable small code base (<1K lines).
-   [SenseDeep](https://www.sensedeep.com) support for visualizing and graphing metrics.

## Database

CustomMetrics stores each metric in a single, compressed DynamoDB item. 

## Quick Tour

Install the library using npm or yarn.

    npm i custom-metrics

Import the CustomMetrics library. If you are not using ES modules or TypeScript, use `require` to import the library.

```javascript
import {CustomMetrics} from 'CustomMetrics'
```

Next create and configure the CustomMetrics instance. 

```javascript
// Create OneTable instance for your DynamoDB table
const metrics = new CustomMetrics({
    client: dynamoDbClient,
    owner: 'my-service',
    tableName: 'MyTable',
    primaryKey: 'pk',
    sortKey: 'sk',
})
```

Metrics emitted by an instance will be scoped and "owned" by the `owner` property you specify. This is typically a service, application or account name. CustomMetric instances with different owners are isolated from each other and their metrics will not interfere with each other. If omitted, the owner defaults to 'account'.

Metrics are stored in the DynamoDB database referenced by the **dynamodDbClient** instance which is an AWS V3 DynamoDB Document Client instance.

```javascript
import {DynamoDBClient} from '@aws-sdk/client-dynamodb'

const dynamoDbClient = new DynamoDBClient()
```

## OneTable

Alternatively, if you are using [OneTable](https://www.npmjs.com/package/dynamodb-onetable) you can construct CustomMetrics using your OneTable instance. In this case, the table name and primary/sort keys are inferred from the OneTable instance.

```javascript
// Create OneTable instance for your DynamoDB table
const metrics = new CustomMetrics({
    onetable: OneTableInstance,
    owner: 'my-service',
})
```

You can emit metrics via:

```javascript
await metrics.emit('Acme/Metrics', 'launches', 10)
```

This will emit the `launches` metric in the `Acme/Metrics` namespace with the value of 10. 

A metric can have dimensions that are unique metrics for a specific instance. For example, we may want to count the number of launches for a specific rocket.

```javascript
await metrics.emit('Acme/Metrics', 'launches', 10, [{rocket: 'saturnV'}])
```

The metric will be emitted for each dimension provided. A dimension may have multiple properties.

If you want to emit a metric without dimensions, you can add {}. For example:

```javascript
await metrics.emit('Acme/Metrics', 'launches', 10, [{}, {rocket: 'saturnV'}])
await metrics.emit('Acme/Metrics', 'launches', 10, [{}, {rocket: 'falcon9'}])
```

To query a metric, use the `query` method:

```javascript
let results = await metrics.query('Acme/Metrics', 'speed', {rocket: 'saturnV'}, 'mth', 'max')
```

This will retrieve the 'speed' metric from the 'Acme/Metrics' namespace for the rocket == 'saturnV' dimension. The data points returned will be the maximum speed measured over the month during each interval. By default, the interval for the month span is 2 days.

This will return data like this:

```json
{
    "namespace": "Acme/Metrics",
    "metric": "launches",
    "dimensions": {rocket: "saturnV"},
    "spans": [{
        "end": 946648800,
        "period": 300,
        "samples": 10,
        "points": [
            { "sum": 24000, "count": 19, "min": 1000, "max": 5000 },
            ...
        ]
    ]
}
```

If you want to query the results as a single value over the entire period (instead of as a set of data points), set the "accumulate" options to true.

```javascript
let results = await metrics.query('Acme/Metrics', 'speed', {rocket: 'saturnV'}, 86400, 'max', {accumulate: true})
```

This will return a single maximum speed over the last day.

To obtain a list of metrics, use the `getMetricList` method:

```typescript
let list: MetricList = await metrics.getMetricList()
```

This will return an array of available namespaces in **list.namespaces**.

To get a list of the metrics available, pass a metric as the first argument.

```typescript
let list: MetricList = await metrics.getMetricList('Acme/Metrics')
```

This will return a list of metrics in **list.metrics**.

To get a list of the dimensions available for a metric, pass in a namespace and metric.

```typescript
let list: MetricList = await metrics.getMetricList('Acme/Metrics', "speed")
```

This will return a list of dimensions in **list.dimensions**.

In all calls, the full list of namespaces will be returned regardless.

<!--
### Viewing Metrics

The [SenseDeep](https://www.sensedeep.com) studio can create graphical dashboards with widgets to display CustomMetrics or standard AWS metrics.
-->

<!--
## Must cover

* Custom spans
* Custom P resolution
* Using P values
* raw query
* PK/SK
* owners
* ttl
* implementation (structure of data items)
    - aging metrics
    - collisions
* Calculating max, min, sum, count, ...
* controlling cost

### Benchmarks

Because CustomMetrics stores metrics in a compressed single DynamoDB item. Emitting a metric, typically uses only a single DynamoDB write and consumes only 1 WCU.

Here are the results of benchmarks against standard CloudWatch Metrics.

CustomMetrics is 1000x times cheaper than CloudWatch Metrics:

| Logger             |   Time   | Code Size  |
| ------------------ | :------: | ---------- |
| CustomMetrics       |  477 ms  | 450 lines  |
| CloudWatch Metrics | 3,269 ms | 1281 lines |
-->

## Limitations

While CustomMetrics does have options to buffer and coalesce metric updates, CustomMetrics can impose a meaningful DynamoDB write load if you are updating metrics extremely frequently. See [Buffering](#buffering) below for mitigations.

## Metric Schema

CustomMetrics are stored in a DynamoDB table using the following single-table schema. 

```
const Schema = {
    format: 'onetable:1.1.0',
    version: '0.0.1',
    indexes: {primary: {hash: 'pk', sort: 'sk'}},
    models: {
        Metric: {
            pk: {type: 'string', value: 'metric#${version}#${owner}'},
            sk: {type: 'string', value: 'metric#${namespace}#${metric}#${dimensions}'},
            dimensions: {type: 'string', required: true, encode: ['sk', '#', '3']},
            expires: {type: 'date', ttl: true},
            metric: {type: 'string', required: true, encode: ['sk', '#', '2']},
            namespace: {type: 'string', required: true, encode: ['sk', '#', '1']},
            owner: {type: 'string', required: true, encode: ['pk', '#', '2']},
            version: {type: 'number', default: Version, encode: ['pk', '#', '1']},
            spans: {
                type: 'array',
                required: true,
                default: [],
                items: {
                    type: 'object',
                    default: {},
                    schema: {
                        // When the points will be full.
                        end: {type: 'number', required: true, map: 'se'}, 
                        period: {type: 'number', required: true, map: 'sp'},
                        samples: {type: 'number', required: true, map: 'ss'},
                        points: {
                            type: 'array',
                            required: true,
                            map: 'pt',
                            default: [],
                            items: {
                                type: 'object',
                                schema: {
                                    count: {type: 'number', required: true, map: 'c'},
                                    max: {type: 'number', map: 'x'},
                                    min: {type: 'number', map: 'm'},
                                    sum: {type: 'number', required: true, map: 's'},
                                    //  P-values and timestamp are not stored
                                    pvalues: {type: 'array', map: 'v'},
                                    timestamp: {type: 'number', map: 'e', },
                                },
                            },
                        },
                    },
                },
            },
            seq: {type: 'number', default: 0},
            _source: {type: 'string'}, // When set, bypass DynamoDB steams change detection
        }
    } as const,
    params: {
        partial: true,
        isoDates: true,
        nulls: false,
        timestamps: false,
        typeField: '_type',
    },
}
```

### CustomMetrics Class API

The CustomMetrics class provides the public API for CustomMetrics and public properties.

### CustomMetrics Constructor

```javascript
const metrics = new CustomMetrics({
    onetable: db,
    owner: 'my-service',
})
```

The CustomMetrics constructor takes an options parameter and an optional context property.

The `options` parameter is of type `object` with the following properties:

| Property | Type | Description | 
| -------- | :--: |------------ |
| buffer | `object` | Buffer metric emits. Has properties: {count, elapsed, sum}
| client | `object` | AWS DynamoDB client instance
| log | `object` | Logging object with methods for 'info', 'error' and 'warn'
| onetable | `OneTable Table object` | OneTable instance to communicate with DynamoDB
| owner | `string` | Unique owner of the metrics. This is used to compute the primary key for the metric data item.
| primaryKey | `string` | Name of the DynamoDB table primary key attribute. Defaults to 'pk'.
| sortKey | `string` | Name of the DynamoDB table sort key attribute. Defaults to 'sk'.
| prefix | `string` | Primary and sort key prefix to use. Defaults to 'metric#'.
| pResolution | `number` | Number of values to store to compute P value statistics. Defaults to zero.
| source | `string` | Reserved
| spans | `array` | Array of span definitions. See below.
| tableName | `string` | Name of the DynamoDB table to use. Required if using `client` instead of `onetable` options.
| typeField | `array` | Onetable attribute used to store the model type. Defaults to `_type`.
| ttl | `number` | Maximum lifespan of the metrics.

For example:

```javascript
const log = new CustomMetrics({
    onetable: onetable,
    owner: 'my-service',
    pResolution: 100,
    ttl: 6 * 24 * 86400,
})
```

CustomMetric spans define how each metric is processed and aged. The spans are an ordered list of metric interval periods. For example, the default spans calculate statistics for the periods: 5 minutes, 1 hour, 1 day, 1 week, 1 month and 1 year. 

Via the `spans` constructor option you can provide an alternate list of spans for higher, lower or more granular resolution.

The default CustomMetrics spans are:

```typescript
const DefaultSpans: SpanDef[] = [
    {period: 5 * 60, samples: 10}, //  5 mins, interval: 30 secs
    {period: 60 * 60, samples: 12}, //  1 hr, interval: 5 mins
    {period: 24 * 60 * 60, samples: 12}, //  24 hrs, interval: 2 hrs
    {period: 7 * 24 * 60 * 60, samples: 14}, //  7 days, interval: 1/2 day
    {period: 28 * 24 * 60 * 60, samples: 14}, //  28 days, interval: 2 days
    {period: 365 * 24 * 60 * 60, samples: 12}, //  1 year, interval: 1 month
]
```

The span `period` property is the number of seconds in that span. The `samples` property specifies the number of data points to be captured. The (period / points) value is the interval between computed data points. If you call emit() more frequently than this, CustomMetrics will agregate the extra values into the relevant span value.

Here is an example of a higher resolution set of spans that keep metric values for 1 minute, 5 minutes, 1 hour and 1 day.

```typescript
const log = new CustomMetrics({
    onetable: onetable,
    owner: 'my-service',
    spans: [
        {period: 1 * 60, samples: 5}, //  interval: 5 secs
        {period: 5 * 60, samples: 10}, //  interval: 30 secs
        {period: 60 * 60, samples: 12}, //  interval: 5 mins
        {period: 24 * 60 * 60, samples: 12}, //  interval: 2 hrs
    ]
})
```

## Buffering

If you have a metric that your app emits metrics at very high frequency, you may wish to optimize metrics by aggregating updates. CustomMetrics can aggregate metric updates by buffering emit calls. These are then persisted depending on your configured buffering policy.

For example:

```javascript
await metrics.emit('Acme/Metrics', 'DataSent', 123, [], {
    buffer: {sum: 1024, count: 20, elapsed: 60}
})
```

This will buffer metric updates in-memory until the sum of buffered `DataSent` is greater than 1024, or there have been 20 calls to emit, or 60 seconds has elapsed, whichever is reached first.  If elapsed is omitted, the default elapsed period is the period of your lowest span.  CustomMetrics will regularly flush metrics as required and will save buffered metrics upon Lambda instance termination. 

Buffered metrics may be less accurate than non-buffered metrics. Metrics may be retained in-memory for a period of time before being flushed to DynamoDB. If a Lambda instance is not required to service a request, any buffered metrics will remain in-memory until AWS terminates the Lambda -- whereupon the buffered values will be saved. This may mean a temporary loss of accuracy to querying entities.

Furthermore, if you have a very large number of metrics in one Lambda instance, it is possible that the Lambda instance may not be able to save all buffered metrics during the Lambda termination timeout. This can be somewhat mitigated by using shorter buffering criteria.

For these reasons, don't use buffered metrics if you require absolute precision. But if you have metrics where less than perfect accuracy is acceptable, then buffered metrics can give very large performance gains.

## Methods

### emit

Emit one or more metrics.

```typescript
async emit(namespace: string, 
    metric: string, 
    value: number, 
    dimensions: MetricDimensionsList = [{}],
    options?: {
        buffer: {
            sum: number, 
            count: number, 
            elapsed: number,
        }
        timestamp?: number
    }): Promise<void>
```

This call will emit metrics for each of the specified dimensions using the supplied namespace and metric name. These will be combined with the CustomMetrics owner supplied via the constructor to scope the metric. 

For example:

```typecript
await metrics.emit('Acme/Metrics', 'launches', 10, 
    [{}, {rocket: 'saturnV'}, {mission: 'ISS-service'}])
```

This will create three metrics:
|Namespace|Metric|Dimensions|
|-|-|-|
|Acme/Metrics|launches|All|
|Acme/Metrics|launches|rocket == saturnV|
|Acme/Metrics|launches|mission == ISS-service|

The `buffer` option can be provided to optimize metric load by aggregating calls to emit(). See [Buffering](#buffering) for details.

### query

Query a metric value.

```typescript
async query(namespace: string, 
    metricName: string, 
    dimensions: MetricDimensions, 
    period: number, 
    statistic: string, 
    options: MetricQueryOptions,
    Promise<MetricQueryResult>
```

This will retrieve a metric value for a given namespace, metric name and set of dimensions.

The `period` argument selects the metric span name to query. For example: 3600 for one hour.

The `statistic` can be `avg`, `max`, `min`, `sum`, `count` or a P-value of the form `pNN` where NN is the P-value. For example: p95 would return the P-95 value. To get meaningful P-value statistics you must set the CustomMetrics pResolution parameter to the number of data points to keep for computing P-values. By default this resolution is zero, which means P-values are not computed. To enable, you should set this to at least 100.


### getMetricList

Return a list of supported namespaces, metrics and dimensions.

```typescript
async getMetricList(
    namespace: string | undefined, 
    metric: string | undefined, 
    options = {fields, limit}): Promise<MetricList>
```

This call will return a MetricList of the form:

```typescript
type MetricList = {
    namespaces: string[]
    metrics?: string[]
    dimensions?: MetricDimensions[]
}
```

The list of namespaces will always be returned. If a namespace argument is provided, the list of metrics in that namespace will be returned. If a metric argument is provided, the list of dimensions for that metric will be returned.

### References

-   [SenseDeep Blog](https://www.sensedeep.com/blog/)
-   [SenseDeep Web Site](https://www.sensedeep.com/)
-   [SenseDeep Developer Studio](https://app.sensedeep.com/)

### Participate

All feedback, discussion, contributions and bug reports are very welcome.

-   [discussions](https://github.com/sensedeep/CustomMetrics/discussions)
-   [issues](https://github.com/sensedeep/CustomMetrics/issues)

### SenseDeep

A great way to view CustomMetrics is with [SenseDeep](https://www.sensedeep.com/). You can create dashboards with graphs, gauges and numerical widgets to display, monitor and alert on your metrics.

### Contact

You can contact me (Michael O'Brien) on Twitter at: [@mobstream](https://twitter.com/mobstream), and read my [Blog](https://www.sensedeep.com/blog).

