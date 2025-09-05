/*
    CustomMetrics - Simple, configurable, economical metrics for AWS

    See the README for the Metric schema details.
 */
import process from 'process'
import {
    DynamoDBClient,
    DynamoDBClientConfig,
    GetItemCommand,
    PutItemCommand,
    PutItemCommandInput,
    QueryCommand,
} from '@aws-sdk/client-dynamodb'
import {marshall, unmarshall} from '@aws-sdk/util-dynamodb'

const Version = 1
const Assert = false // Enable asserts
const Buffering = true // Enable buffered metrics
const DefaultResolution = 0 // Default number of p-values to store
const MaxSeq = Number.MAX_SAFE_INTEGER // Maximum sequence number for collision detection
const MaxRetries = 10 // Max retries when emitting a metric and encountering collisions
const MetricListLimit = 10000 // Sanity limit for getting a list of metrics

export type SpanDef = {
    period: number // Span's total timespan
    samples: number // Number of data points in period
}

/*
    Default spans are configurable by the constructor
 */
export const DefaultSpans: SpanDef[] = [
    {period: 5 * 60, samples: 10}, //  300, 5 mins, interval: 30 secs
    {period: 60 * 60, samples: 12}, //  3600, 1 hr, interval: 5 mins
    {period: 24 * 60 * 60, samples: 12}, // 86400, 24 hrs, interval: 2 hrs
    {period: 7 * 24 * 60 * 60, samples: 14}, // 604,800, 7 days, interval: 1/2 day
    {period: 28 * 24 * 60 * 60, samples: 14}, // 2,419,200, 28 days, interval: 2 days
    {period: 365 * 24 * 60 * 60, samples: 12}, // 31,536,000, 1 year, interval: 1 month
]

export type Metric = {
    dimensions: string // Comma separated list of key=value dimensions
    expires?: number // Date in seconds when the item expires since Jan 1, 1970
    id?: string // User ID preserved across queries
    metric: string // Metric name
    namespace: string // Namespace name
    owner?: string // Tenant owner of the metric
    version?: number // Version of this module
    spans: Span[] // Data point spans
    seq?: number // Atomic writing serialization vector
    //  Useful when using streams to filter out items with this attribute
    _source?: string
}

export type Point = {
    count: number //  Count of values in sum
    max?: number
    min?: number
    pvalues?: number[]
    sum: number //  Aggregated values
    timestamp?: number // Buffer timestamp, never stored
}

/*
    A span holds the points for a time span period
    The span.end is one beyond the time of the last possible sample in a points[] bucket. 
    When the span is created, the span.end is set in the future to timestamp + span.interval
    When querying, we often want to return the most recent data points of a span that may not yet 
    be full, i.e. the span.end may be in the future.  
 */
export type Span = {
    end: number //  Timestamp when the span is complete
    period: number //  Span period in seconds
    samples: number //  Number of samples in this span
    points: Point[] //  Data points {count, sum, max, min}
}
/*
    Dimensions are an object of dimension:value properties. 
 */
export type MetricDimensions = {[key: string]: unknown}
export type MetricDimensionsList = MetricDimensions[]

export type MetricList = {
    namespaces: string[]
    metrics?: string[]
    dimensions?: MetricDimensions[]
}

// Type for query result points[]
export type MetricQueryPoint = {
    count: number
    timestamp?: number
    value: number
}

export type MetricQueryResult = {
    dimensions: MetricDimensions
    id?: string
    metric: string
    namespace: string
    owner: string
    period: number
    points: MetricQueryPoint[]
    samples: number
}

// Constructor options
export type MetricOptions = {
    buffer?: MetricBufferOptions
    client?: DynamoDBClient
    consistent?: boolean
    creds?: object
    expires?: string // DynamoDB TTL expires attribute (defaults to "expires")
    log?: true | 'verbose' | any
    owner?: string
    prefix?: string
    pResolution?: number
    primaryKey?: string
    region?: string
    sortKey?: string
    source?: string
    spans?: SpanDef[]
    table?: string
    ttl?: number
    type?: {[key: string]: string} // Optional single type type field
}

export type MetricBufferOptions = {
    sum?: number
    count?: number
    elapsed?: number // Time in seconds to buffer values
    force?: boolean //  Force a flush
}

export type MetricEmitOptions = {
    buffer?: MetricBufferOptions
    log?: boolean
    owner?: string
    timestamp?: number
    ttl?: number
    upgrade?: boolean
}

export type MetricListOptions = {
    log?: boolean
    limit?: number
    owner?: string
    next?: object
    timestamp?: number
}

export type MetricQueryOptions = {
    accumulate?: boolean
    id?: string
    log?: boolean
    owner?: string
    start?: number
    timestamp?: number
}

type BufferElt = {
    count: number //  Number of samples
    dimensions: string //  Dimensions for element
    metricName: string //  Metric for element
    namespace: string //  Namespace for element
    metric: Metric //  Associated metric
    sum: number //  Sum of samples
    timestamp: number // Timestamp of when buffered element should be flushed
    elapsed: number //  Elapsed time to buffer element
}
type BufferMap = {
    [key: string]: BufferElt
}

type InstanceMap = {
    [key: string]: CustomMetrics
}
var Instances: InstanceMap = {}

/*
    On exit, flush any buffered metrics. This requires any Lambda Layer to receive this signal
 */
process.on(
    'SIGTERM',
    /* istanbul ignore next */
    async () => {
        /* istanbul ignore next */
        await CustomMetrics.terminate()
    }
)

export class CustomMetrics {
    private consistent = false
    private buffer: MetricBufferOptions | undefined
    private buffers: BufferMap = null
    private client: DynamoDBClient
    private expires: string
    private log: any
    private options: MetricOptions
    private owner: string
    private prefix: string = 'metric'
    private primaryKey: string
    private sortKey: string
    private pResolution: number
    private source: string | undefined
    private spans: SpanDef[]
    private table: string
    private type: {[key: string]: string}
    private ttl: number

    constructor(options: MetricOptions = {}) {
        this.log = new Log(options.log)
        if (options.ttl && typeof options.ttl != 'number') {
            throw new Error('Bad type for "ttl" option')
        }
        if (options.spans && (!Array.isArray(options.spans) || options.spans.length == 0)) {
            throw new Error('The "spans" option must be an non-empty array')
        }
        if (options.source && typeof options.source != 'string') {
            throw new Error('Non-string "source" option')
        }
        if (options.pResolution != undefined && (options.pResolution < 0 || options.pResolution > 1000)) {
            throw new Error('Invalid "pResolution" option. Must be between 0 and 1000. Default is 0')
        }
        if (options.consistent != null && typeof options.consistent != 'boolean') {
            throw new Error('Bad type for "consistent" option')
        }
        if (options.prefix) {
            this.prefix = options.prefix
        }
        if (options.buffer) {
            if (typeof options.buffer != 'object') {
                throw new Error('Bad type for "buffer" option')
            }
            this.buffer = options.buffer
        }
        this.expires = options.expires || 'expires'
        this.primaryKey = options.primaryKey || 'pk'
        this.sortKey = options.sortKey || 'sk'
        this.type = options.type || {_type: 'Metric'}

        /* istanbul ignore else */
        if (options.client) {
            this.client = options.client
        } else {
            let params: DynamoDBClientConfig = {}
            if (options.creds) {
                params.credentials = options.creds as any
                //  Allow region in credentials
                params.region = (params.credentials as any).region
            }
            if (options.region) {
                params.region = options.region
            }
            this.client = new DynamoDBClient(params)
        }
        if (!options.table) {
            throw new Error('Missing DynamoDB table name property')
        }
        /* istanbul ignore next */
        this.table = options.table

        this.options = options
        this.owner = options.owner || 'default'
        this.spans = options.spans || DefaultSpans
        this.ttl = options.ttl || this.spans[this.spans.length - 1].period

        if (options.consistent != null) {
            this.consistent = options.consistent
        }
        if (options.source) {
            this.source = options.source
        }
        this.pResolution = options.pResolution || DefaultResolution
    }

    async emit(
        namespace: string,
        metricName: string,
        value: number,
        dimensionsList: MetricDimensionsList = [{}],
        options: MetricEmitOptions = {}
    ): Promise<Metric> {
        if (value == undefined || value == null) {
            throw new Error('Invalid metric value')
        }
        if (dimensionsList.length == 0) {
            dimensionsList = [{}]
        }
        value = Number(value)
        if (isNaN(value)) {
            throw new Error(`Value to emit is not valid`)
        }
        if (!namespace || !metricName) {
            throw new Error('Missing emit namespace / metric argument')
        }
        /* istanbul ignore next */
        if (!Array.isArray(dimensionsList)) {
            throw new Error('Dimensions must be an array')
        }
        if (dimensionsList.length == 0) {
            dimensionsList = [{}]
        }
        let point: Point
        point = {count: 1, sum: value}
        return await this.emitDimensions(namespace, metricName, point, dimensionsList, options)
    }

    /*
        Emit a metric for each dimension in the dimensionsList
     */
    private async emitDimensions(
        namespace: string,
        metricName: string,
        point: Point,
        dimensionsList: MetricDimensionsList,
        options: MetricEmitOptions
    ): Promise<Metric> {
        let result: Metric
        for (let dim of dimensionsList) {
            let dimensions = this.makeDimensionString(dim)
            let buffer = options.buffer || this.buffer
            if (buffer && (buffer.elapsed || buffer.force || buffer.sum || buffer.count) && Buffering) {
                result = await this.bufferMetric(namespace, metricName, point, dimensions, options)
            } else {
                result = await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options)
            }
        }
        return result!
    }

    /*
        Buffer a metric for specific dimensions
        Point values are uniquely buffered in elements indexed by a (namespace, metric, dimensions) key.
        Buffered values are flushed when a sum, count or timespan parameter is exceeded.
        The accumulated metric is returned but will only contain buffered values for this Lambda.
        When the metric is flushed, the full persisted metric is returned with updated spans.
        This means returned metrics will typically be just accumulated buffered values and won't reflect
        other lambdas until flushed. If you need consistent return values -- use query().
        The return value has a usable Metric with normalized spans and values.
     */
    async bufferMetric(
        namespace: string,
        metricName: string,
        point: Point,
        dimensions: string,
        options: MetricEmitOptions
    ): Promise<Metric> {
        let buffer = options.buffer || this.buffer
        let key = this.getBufferKey(namespace, metricName, dimensions)
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)
        let owner = options.owner || this.owner

        let elt: BufferElt = await this.getBufferedMetric(owner, namespace, metricName, dimensions, options)
        if (
            buffer.force ||
            (buffer.sum && elt.sum + point.sum >= buffer.sum) ||
            (buffer.count && elt.count + point.count >= buffer.count) ||
            timestamp >= elt.timestamp
        ) {
            /*
                Time to persist the metric.
             */
            let metric = await this.emitDimensionedMetric(
                namespace,
                metricName,
                {count: elt.count, sum: elt.sum, timestamp: elt.timestamp},
                dimensions,
                Object.assign({}, options, {timestamp: timestamp * 1000})
            )
            elt.metric = metric
            //  Reset tallies and save higher spans to return for future buffered metrics
            elt.count = elt.sum = 0
            elt.timestamp = timestamp + (buffer.elapsed || this.spans[0].period / this.spans[0].samples)
        }
        /*
            Update the element and also add to the spans so we can return the full metric
         */
        elt.count += point.count
        elt.sum += point.sum
        this.addValue(elt.metric.spans, point, timestamp)

        CustomMetrics.saveInstance({key}, this)
        return elt.metric
    }

    /*
        Get a buffered metric from the in-memory cache.
        First time, read from DynamoDB to get the current span values.
     */
    async getBufferedMetric(
        owner: string,
        namespace: string,
        metricName: string,
        dimensions: string,
        options: MetricEmitOptions
    ): Promise<BufferElt> {
        let buffers = (this.buffers = this.buffers || {})
        let key = this.getBufferKey(namespace, metricName, dimensions)
        let elt: BufferElt = buffers[key]
        if (elt) {
            return elt
        }
        let buffer = options.buffer || this.buffer
        let elapsed = buffer.elapsed || this.spans[0].period / this.spans[0].samples
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)

        let metric = await this.getMetric(owner, namespace, metricName, dimensions, options.log)
        if (!metric) {
            metric = this.initMetric(owner, namespace, metricName, dimensions, timestamp)
        }
        elt = {
            count: 0,
            sum: 0,
            timestamp: timestamp + elapsed,
            elapsed: elapsed,
            namespace: namespace,
            metricName: metricName,
            metric: metric,
            dimensions,
        }
        buffers[key] = elt
        return elt
    }

    /*
        Emit a metric for specific dimensions
     */
    private async emitDimensionedMetric(
        namespace: string,
        metricName: string,
        point: Point,
        dimensions: string,
        options: MetricEmitOptions = {}
    ): Promise<Metric> {
        /*
            Update the metric. May need retries if colliding with other updaters.
         */
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)
        let ttl = options.ttl != undefined ? options.ttl : this.ttl
        let retries = MaxRetries
        let metric: Metric | undefined
        let backoff = 10
        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        do {
            let owner = options.owner || this.owner
            metric = await this.getMetric(owner, namespace, metricName, dimensions, options.log)
            if (metric) {
                if (options.upgrade) {
                    metric = this.upgradeMetric(metric)
                }
            } else {
                metric = this.initMetric(owner, namespace, metricName, dimensions, timestamp)
            }
            this.addValue(metric.spans, point, timestamp)

            /* istanbul ignore next */
            if (this.source) {
                metric._source = this.source
            }
            /*
                Set the expiration TTL. Defaults to the longest span.
                Users may define a shorter TTL to prune metrics for inactive items.
            */
            if (ttl) {
                //  Now and ttl are in seconds
                metric.expires = timestamp + ttl
            }
            if (await this.putMetric(metric, options)) {
                break
            }
            /* istanbul ignore next */
            if (retries == 0) {
                this.log.error(`Metric update has too many retries`, {namespace, metricName, dimensions})
                break
            }
            /* istanbul ignore next */
            this.log[chan](
                `Retry ${MaxRetries - retries} metric update ${metric.namespace} ${metric.metric} ${metric.dimensions}`,
                {
                    retries,
                    metric,
                }
            )
            //  Exponential backoff
            /* istanbul ignore next */
            backoff = backoff * 2
            /* istanbul ignore next */
            this.log[chan](`Retry backoff ${backoff} ${this.jitter(backoff)}`)
            /* istanbul ignore next */
            await this.delay(this.jitter(backoff))
        } while (retries-- > 0)
        return metric
    }

    /*
        Upgrade a metric with new spans. Only the specified dimensions are upgraded.
     */
    async upgrade(
        namespace: string,
        metricName: string,
        dimensionsList: MetricDimensionsList = [{}],
        options: MetricEmitOptions = {}
    ): Promise<Metric> {
        let owner = options.owner || this.owner
        if (dimensionsList.length == 0) {
            dimensionsList = [{}]
        }
        let metric
        for (let dim of dimensionsList) {
            let dimensions = this.makeDimensionString(dim)
            let old = await this.getMetric(owner, namespace, metricName, dimensions, options.log)
            metric = this.upgradeMetric(old)
            await this.putMetric(metric, options)
        }
        return metric
    }

    /*
        Upgrade a metric and return the upgraded result
        Optimized for when an upgrade is not required.
     */
    upgradeMetric(old: Metric): Metric {
        let required = false
        /*
            Check if upgrade required
         */
        if (this.spans.length == old.spans.length) {
            for (let [index, span] of Object.entries(old.spans)) {
                if (span.period != this.spans[index].period || span.samples != this.spans[index].samples) {
                    required = true
                }
            }
            if (!required) {
                return old
            }
        }
        /*
            This initializes a new metric with the new spans and apportion the old data points to 
            the new metric at the point's timestamp. Pick the earliest timestamp from the old metric.
         */
        let timestamp = Math.min(...old.spans.map((span) => span.end - span.period)) || Math.floor(Date.now() / 1000)
        let metric = this.initMetric(old.owner, old.namespace, old.metric, old.dimensions, timestamp)
        for (let span of old.spans) {
            let interval = span.period / span.samples
            let start = span.end - span.points.length * interval
            /*
                Pick the first span for which the point is after the earliest span start or after the end of the span
            */
            let si = metric.spans.findIndex((s) => s.end - s.period <= start || s.end <= start)
            for (let point of span.points) {
                this.addValue(metric.spans, point, start, si)
                start += interval
            }
        }
        return metric
    }

    /*
        Flush metrics for all instances on Lambda termination
     */
    static async terminate() {
        await CustomMetrics.flushAll()
    }

    static async flushAll() {
        for (let [key, instance] of Object.entries(Instances)) {
            await instance.flush()
            CustomMetrics.freeInstanceByKey(key)
        }
        Instances = {}
    }

    async flush(options: MetricQueryOptions = {}) {
        if (!this.buffers) return
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)
        for (let elt of Object.values(this.buffers)) {
            await this.flushElt(elt, timestamp)
        }
    }

    async flushElt(elt: BufferElt, timestamp: number) {
        //  Choose timestamp if before the buffer expires, otherwise choose the buffer expiry time
        elt.timestamp = Math.min(timestamp, elt.timestamp)
        let metric = await this.emitDimensionedMetric(elt.namespace, elt.metricName, elt, elt.dimensions, {
            timestamp: elt.timestamp * 1000,
        })
        elt.count = elt.sum = 0
        elt.metric = metric
        elt.timestamp = timestamp + (elt.elapsed || this.spans[0].period / this.spans[0].samples)
    }

    getBufferKey(namespace: string, metricName: string, dimensions: string): string {
        return `${namespace}|${metricName}|${JSON.stringify(dimensions)}`
    }

    /*
        Query metrics. Return an array of metrics
        This flushes buffered metrics for this specific metric.
     */
    async query(
        namespace: string,
        metricName: string,
        dimensions: MetricDimensions,
        period: number,
        statistic: string,
        options: MetricQueryOptions = {}
    ): Promise<MetricQueryResult> {
        let owner = options.owner || this.owner
        let dimString = this.makeDimensionString(dimensions)

        if (period > this.spans.at(-1).period) {
            period = this.spans.at(-1).period
        }
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)

        /*
           Flush buffered metrics for this instance. Will not see buffered metrics in other instances 
           until they are flushed.
         */
        if (this.buffers) {
            let key = this.getBufferKey(namespace, metricName, dimString)
            if (this.buffers[key]) {
                await this.flushElt(this.buffers[key], timestamp)
            }
        }
        let metric = await this.getMetric(owner, namespace, metricName, dimString, options.log)
        if (!metric) {
            return {dimensions, id: options.id, metric: metricName, namespace, period, points: [], owner, samples: 0}
        }
        let result = this.processMetric(metric, period, statistic, timestamp, options)

        /* istanbul ignore next */
        this.log[options.log == true ? 'info' : 'trace'](`Query metrics ${namespace}, ${metricName}`, {
            dimensions,
            period,
            statistic,
            options,
            result,
        })
        return result
    }

    async queryMetrics(
        namespace: string,
        metric: string | undefined,
        period: number,
        statistic: string,
        options: MetricListOptions = {}
    ): Promise<MetricQueryResult[]> {
        let owner = options.owner || this.owner
        let next: object | undefined = options.next
        let limit = options.limit || MetricListLimit
        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        let items, command
        let count = 0
        do {
            /* istanbul ignore next */
            ;({command, items, next} = await this.findMetrics(owner, namespace, metric, limit, next, 'spans'))
            this.log[chan](`Find metrics ${namespace}, ${metric}`, {command, items})
            if (items.length) {
                count += items.length
            }
        } while (next && count < limit)

        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000)

        let results = []
        for (let metric of items) {
            let result = this.processMetric(metric, period, statistic, timestamp, {accumulate: true, timestamp})
            results.push(result)
        }
        return results
    }

    /*
       Process a metric for query() or queryMetrics() and extract the desired series or accumlated value
     */
    processMetric(
        metric: Metric,
        period: number,
        statistic: string,
        timestamp: number,
        options: MetricQueryOptions
    ): MetricQueryResult {
        let end: number
        let si: number
        let owner = options.owner || this.owner

        if (options.start) {
            /*
                Find span for the request start: period < span[i+1].period
            */
            let start = options.start / 1000
            si = metric.spans.findIndex((s) => period <= s.period && s.end - s.period <= start && start < s.end)
            end = start + period
        } else {
            let span = metric.spans[0]
            let interval = span.period / span.samples
            /*
                Special case: if now is within the most recent span interval, then set "start" to include this interval.
            */
            if (span.end - interval <= timestamp && timestamp < span.end) {
                end = span.end
            } else {
                end = timestamp
            }
            si = metric.spans.findIndex((s) => period <= s.period)
        }
        if (si < 0) {
            si = metric.spans.length - 1
        }
        /*
            Aggregate data for all spans up to the desired span. Do this because spans are updated lazily on emit.
            For accumulated 'current' queries, we search for the first current value from span 0 upwards.
         */
        if (statistic == 'current' && options.accumulate) {
            si = 0
        }
        /*
            Normalize the spans for the current timestamp
         */
        this.addValue(metric.spans, {count: 0, sum: 0}, timestamp, 0, si)

        let span = metric.spans[si]
        let result: MetricQueryResult
        if (options.accumulate) {
            result = this.accumulateMetric(metric, span, statistic, owner, end, period)
        } else {
            result = this.calculateSeries(metric, span, statistic, owner, end, period)
        }
        result.id = options.id
        return result
    }

    /*
        Accumulate the metric data points into a single value. 
        This is useful for gauges and widgets that need a single data value for the metric.
        According to the desired statistic, this calculates the avg, count, max, min, sum, and pvalues.
     */
    private accumulateMetric(
        metric: Metric,
        span: Span,
        statistic: string,
        owner: string,
        end: number,
        period: number
    ): MetricQueryResult {
        let start = this.alignTime(span, end - period)
        let value: number = 0,
            count: number = 0,
            pvalues: number[] = []
        if (statistic == 'max') {
            value = Number.NEGATIVE_INFINITY
        } else if (statistic == 'min') {
            value = Infinity
        } else if (statistic == 'sum') {
            value = 0
            count = 0
        } else if (statistic == 'count') {
            value = 0
            count = 0
        } else if (statistic == 'current') {
            value = 0
            count = 0
        } else if (statistic.match(/^p[0-9]+/)) {
            pvalues = []
        } /* avg */ else {
            value = 0
            count = 0
        }
        let points = span.points
        let interval = span.period / span.samples
        let t = span.end - span.points.length * interval

        if (statistic == 'current') {
            // Find the most recent point with a count > 0 and use it to calculate the current value.
            for (let s of metric.spans) {
                for (let p of s.points.reverse()) {
                    if (p.count > 0) {
                        value = p.sum / p.count
                        count = p.count
                        break
                    }
                }
                if (count > 0) break
            }
        } else {
            for (let i = 0; i < points.length; i++) {
                let point = points[i]
                if (start <= t && t < start + period) {
                    if (statistic == 'max') {
                        if (point.max != undefined) {
                            value = Math.max(value, point.max)
                        } else {
                            //  For use to accumulate AWS metrics that don't keep min/max in points
                            value = Math.max(value, point.sum / (point.count || 1))
                        }
                    } else if (statistic == 'min') {
                        if (point.min != undefined) {
                            value = Math.min(value, point.min)
                        } else {
                            //  For use to accumulate AWS metrics that don't keep min/max in points
                            value = Math.min(value, point.sum / (point.count || 1))
                        }
                    } else if (statistic == 'sum') {
                        value += point.sum
                    } else if (statistic == 'count') {
                        value += point.count
                    } else if (statistic.match(/^p[0-9]+/)) {
                        pvalues = pvalues.concat(point.pvalues)
                    } /* avg */ else {
                        value += point.sum
                    }
                    count += point.count
                }
                t += interval
            }
        }
        if (statistic.match(/^p[0-9]+/)) {
            let p = parseInt(statistic.slice(1))
            pvalues.sort((a, b) => a - b)
            let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
            value = pvalues[nth]
        } else if (statistic == 'avg') {
            value /= Math.max(count, 1)
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{value, timestamp: start + period, count}],
            samples: span.samples,
        }
    }

    /*
        Process the metric points. This is used for graphs that need all the data points.
        This calculates the avg, max, min, sum, count, pvalues and per-point timestamps.
        This always returns a full series of points even if there is no data.
     */
    private calculateSeries(
        metric: Metric,
        span: Span,
        statistic: string,
        owner: string,
        end: number,
        period: number
    ): MetricQueryResult {
        let points: MetricQueryPoint[] = []
        let interval = span.period / span.samples

        /*
            Start points aligned with span buckets
         */
        let start = this.alignTime(span, end - period)
        let firstPoint = span.end - span.points.length * interval

        let t: number
        for (t = start; t < firstPoint && points.length < span.samples; t += interval) {
            points.push({value: 0, count: 0, timestamp: t * 1000})
        }
        t = firstPoint
        for (let point of span.points) {
            if (start <= t && t < end) {
                let value: number = undefined
                /* istanbul ignore else */
                if (point.count > 0) {
                    if (statistic == 'max') {
                        if (point.max != undefined) {
                            if (value == undefined) {
                                value = point.max
                            } else {
                                value = Math.max(value, point.max)
                            }
                        }
                    } else if (statistic == 'min') {
                        if (point.min != undefined) {
                            if (value == undefined) {
                                value = point.min
                            } else {
                                value = Math.min(value, point.min)
                            }
                        }
                    } else if (statistic == 'sum') {
                        value = point.sum
                    } else if (statistic == 'count') {
                        value = point.count
                    } else if (statistic.match(/^p[0-9]+/)) {
                        let p = parseInt(statistic.slice(1))
                        let pvalues = point.pvalues
                        pvalues.sort((a, b) => a - b)
                        let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
                        value = pvalues[nth]
                    } /* avg | current */ else {
                        // NOTE: avg and current are the same for series
                        value = point.sum / point.count
                    }
                } else {
                    value = 0
                }
                /*
                    The timestamp is set to be the end of the point bucket not the start, and not after the range
                */
                let timestamp = Math.min(t + interval, end) * 1000
                points.push({value, count: point.count, timestamp})
            }
            t += interval
        }
        let count = Math.min(Math.ceil(period / interval), span.samples)
        while (points.length < count) {
            let timestamp = Math.min(t + interval, end) * 1000
            points.push({value: 0, count: 0, timestamp})
            t += interval
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            period: span.period,
            points: points,
            owner: owner,
            samples: span.samples,
        }
    }

    /*
        Convert a dimensions object of the form {key: value, ...} to a string with comma separated "key=value,..." with sorted property keys
     */
    private makeDimensionString(dimensions: MetricDimensions): string {
        let result: string[] = []
        //  Sort dimension properties
        let entries = Object.entries(dimensions).sort((a, b) => a[0].localeCompare(b[0]))
        for (let [name, value] of entries) {
            result.push(`${name}=${value}`)
        }
        return result.join(',')
    }

    /*
        Convert a dimensions string of the form "key=value,..." to a a dimensions object {key: value, ...}
    */
    private makeDimensionObject(dimensions: string): MetricDimensions {
        let result: MetricDimensions = {}
        for (let dimension of dimensions.split(',')) {
            if (dimension) {
                let [key, value] = dimension.split('=')
                result[key] = value
            }
        }
        return result
    }

    /*
        Update all spans to the latest timestamp and add the given point to the span at the given index.
        Span points are propagated to higher spans as required.
    */
    private addValue(spans: Span[], point: Point, timestamp: number, si: number = 0, queryIndex: number = -1) {
        this.assert(spans)
        this.assert(timestamp)
        this.assert(0 <= si && si < spans.length)

        let span = spans[si]
        let interval = span.period / span.samples
        let points = span.points || []

        //  Just for safety, should not happen
        /* istanbul ignore next */
        while (points.length > span.samples) {
            points.shift()
        }
        let start = span.end - points.length * interval
        let shift = 0

        if (points.length) {
            if (si < queryIndex && si + 1 < spans.length) {
                shift = points.length
            } else if (timestamp >= start) {
                //  Count of aged data points
                shift = Math.floor((timestamp - start) / interval) - span.samples
                if (point.count && timestamp >= span.end) {
                    //  Add one more to make room for this point being added
                    shift += 1
                }
            }
            shift = Math.max(0, Math.min(shift, points.length))
        }
        /*
            Move aged points to the next span 
         */
        if (shift > 0) {
            let t = start
            for (let i = 0; i < shift; i++) {
                let p = points.shift()
                if (p.count && si < spans.length - 1) {
                    p.timestamp = t
                    this.addValue(spans, p, timestamp, si + 1, queryIndex)
                }
                t += interval
            }
        } 
        if (queryIndex >= 0 && si < queryIndex) {
            this.addValue(spans, point, timestamp, si + 1, queryIndex)
            return
        }
        if (si < spans.length - 1) {
            /*
                Check if the point is destined for a higher span
             */
            if (point.timestamp) {
                let elapsed = (timestamp - point.timestamp)
                let target = spans.findIndex(s => s.period >= elapsed)
                if (target > si) {
                    this.addValue(spans, point, timestamp, si + 1, queryIndex)
                    return
                }
            }
            /*
                Aggregate and update higher spans
             */
            if ((si + 1) < spans.length - 1) {
                this.addValue(spans, {count: 0, sum: 0}, timestamp, si + 1, queryIndex)
            }
        }
        /*
            Update the span.end for the current timestamp
         */
        let index = this.updateSpan(span, point, timestamp)

        /*
            Insert any data point values
         */
        if (point.count && index >= 0) {
            this.setPoint(span, index, point)
        }
    }

    /*
        Update the span with room to accomodate the (optional) data point and return the insertion index.
        This will update the span.end if required.
     */
    private updateSpan(span: Span, point: Point, timestamp: number) {
        let interval = span.period / span.samples
        let points = span.points || []
        let start = span.end - points.length * interval
        let when = point.timestamp || timestamp
        /*
            Pad points and determine the right index for the point to be inserted
         */
        let index: number
        if (points.length == 0) {
            if (point.count) {
                points.push({count: 0, sum: 0})
            }
            /*
                This will set the span.end to the next aligned interval that is >timestamp.
                This may mean that span.start is effectively before "now".
            */
            span.end = this.alignTime(span, when + 1)
            index = 0
        } else {
            if (when < span.end - span.period) {
                //  Discard if before the earliest possible point for the span (not an error)
                return -1
            }
            while (when < start) {
                points.unshift({count: 0, sum: 0})
                start -= interval
            }
            /*
                Fill to the point.timestamp or timestamp
             */
            while (when >= span.end && points.length < span.samples) {
                points.push({count: 0, sum: 0})
                span.end += interval
            }
            index = Math.floor((when - start) / interval)
        }
        if (points.length > span.samples) {
            /* Should never happen */
            points = points.slice(-span.samples)
        }
        this.assert(0 <= index && index < points.length)
        return index
    }

    /*
        Add value to a span and update count, min, max, pValues and sum
        The point "index" defines the point[] to update.
     */
    private setPoint(span: Span, index: number, add: Point) {
        let points = span.points
        this.assert(0 <= index && index < points.length)
        let point = points[index]!
        /* istanbul ignore next */
        if (!point) {
            /* Should never happen */
            this.log.error(`Metric null point`, {span, index, add})
            return
        }
        if (add.count) {
            let value = add.sum / add.count
            if (point.min == undefined) {
                point.min = value
            } else {
                point.min = Math.min(value, point.min)
            }
            if (point.max == undefined) {
                point.max = value
            } else {
                point.max = Math.max(value, point.max)
            }
        }
        if (this.pResolution) {
            point.pvalues = point.pvalues || []
            if (add.pvalues) {
                point.pvalues.push(...add.pvalues)
            } else {
                point.pvalues.push(add.sum / add.count)
            }
            point.pvalues.splice(0, point.pvalues.length - this.pResolution)
        }
        point.sum += add.sum
        point.count += add.count
    }

    /*
        Get list of metrics at a given level. The args: namespace and metrics may be undefined.
        Return {namespaces, metrics, dimensions} as possible.
     */
    async getMetricList(
        namespace: string = undefined,
        metric: string = undefined,
        options: MetricListOptions = {limit: MetricListLimit}
    ): Promise<MetricList> {
        let map = {} as any
        let owner = options.owner || this.owner
        let next: object | undefined = options.next
        let limit = options.limit || MetricListLimit
        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        let items, command
        let count = 0
        do {
            /* istanbul ignore next */
            ;({command, items, next} = await this.findMetrics(owner, namespace, metric, limit, next))
            this.log[chan](`Find metrics namespace: ${namespace}, metric: ${metric}`, {command, items})
            if (items.length) {
                for (let item of items) {
                    let ns = (map[item.namespace] = map[item.namespace] || {})
                    let met = (ns[item.metric] = ns[item.metric] || [])
                    met.push(item.dimensions)
                }
                count += items.length
            }
        } while (next && count < limit)

        let result: MetricList = {namespaces: Object.keys(map)}
        if (namespace && map[namespace]) {
            result.metrics = Object.keys(map[namespace])
            if (metric) {
                let dimensions = map[namespace][metric]
                if (dimensions) {
                    result.dimensions = []
                    dimensions = dimensions.sort().filter((v, index, self) => self.indexOf(v) === index)
                    for (let dimension of dimensions) {
                        result.dimensions.push(this.makeDimensionObject(dimension))
                    }
                }
            }
        }
        return result
    }

    private initMetric(owner: string, namespace: string, name: string, dimensions: string, timestamp: number): Metric {
        let metric: Metric = {
            dimensions,
            metric: name,
            namespace,
            owner,
            spans: [],
            version: Version,
        }
        for (let sdef of this.spans) {
            let span: Span = {
                samples: sdef.samples,
                period: sdef.period,
                end: null,
                points: [],
            }
            let interval = span.period / span.samples
            span.end = this.alignTime(span, timestamp + interval - 1)
            metric.spans.push(span)
        }
        return metric
    }

    /*
        Read a metric from DynamoDB
     */
    async getMetric(
        owner: string,
        namespace: string,
        metric: string,
        dimensions: string,
        log: boolean
    ): Promise<Metric> {
        let command = new GetItemCommand({
            TableName: this.table,
            Key: {
                [this.primaryKey]: {S: `${this.prefix}#${Version}#${owner}`},
                [this.sortKey]: {S: `${this.prefix}#${namespace}#${metric}#${dimensions}`},
            },
            ConsistentRead: this.consistent,
        })
        let data = await this.client.send(command)
        let result = null
        if (data && data.Item) {
            let item = unmarshall(data.Item)
            result = this.mapItemFromDB(item)
        }
        if (log == true) {
            let chan = log == true ? 'info' : 'trace'
            this.log[chan](`GetMetric ${namespace}, ${metric} ${dimensions}`, {cmd: command, result})
        }
        return result
    }

    async findMetrics(
        owner: string,
        namespace: string,
        metric: string | undefined,
        limit: number,
        startKey: object,
        fields: string = ''
    ): Promise<{items: Metric[]; next: object; command: QueryCommand}> {
        let key = [namespace]
        if (metric) {
            key.push(metric)
        }
        /* istanbul ignore next */

        let start = startKey ? marshall(startKey) : undefined
        let project = `${this.primaryKey}, ${this.sortKey}`
        if (fields) {
            project += `, ${fields}`
        }
        let command = new QueryCommand({
            TableName: this.table,
            ExpressionAttributeNames: {
                '#_0': this.primaryKey,
                '#_1': this.sortKey,
            },
            ExpressionAttributeValues: {
                ':_0': {S: `${this.prefix}#${Version}#${owner}`},
                ':_1': {S: `${this.prefix}#${key.join('#')}`},
            },
            KeyConditionExpression: '#_0 = :_0 and begins_with(#_1, :_1)',
            ConsistentRead: this.consistent,
            Limit: limit,
            ScanIndexForward: true,
            ExclusiveStartKey: start,
            ProjectionExpression: project,
        })

        let result = await this.client.send(command)

        let items = []
        if (result.Items) {
            for (let i = 0; i < result.Items.length; i++) {
                let item = unmarshall(result.Items[i])
                items.push(this.mapItemFromDB(item))
            }
        }
        let next = undefined
        /* istanbul ignore next */
        if (result.LastEvaluatedKey) {
            next = unmarshall(result.LastEvaluatedKey)
        }
        return {items, next, command}
    }

    /*
        Persist a metric to DynamoDB
        Use a sequence number to detect simultaneous updated. If collision, will throw 
        a ConditionalCheckFailedException and emit() will then retry.
    */
    async putMetric(item: Metric, options: MetricEmitOptions) {
        let ConditionExpression, ExpressionAttributeValues
        let seq: number
        if (item.seq != undefined) {
            /* istanbul ignore next */
            seq = item.seq = item.seq || 0
            /* istanbul ignore next */
            if (item.seq++ >= MaxSeq) {
                item.seq = 0
            }
            ConditionExpression = `seq = :_0`
            ExpressionAttributeValues = {':_0': {N: seq.toString()}}
        } else {
            item.seq = 0
        }
        let mapped = this.mapItemToDB(item)
        let params: PutItemCommandInput = {
            TableName: this.table,
            ReturnValues: 'NONE',
            Item: marshall(mapped, {removeUndefinedValues: true}),
            ConditionExpression,
            ExpressionAttributeValues,
        }
        let command = new PutItemCommand(params)

        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        this.log[chan](`Put metric ${item.namespace}, ${item.metric}`, {
            dimensions: item.dimensions,
            command,
            params,
            item,
        })
        try {
            await this.client.send(command)
            return true
        } catch (err) /* istanbul ignore next */ {
            ;(function (err, log) {
                //  SDK V3 puts the code in err.name (Ugh!)
                let code = err.code || err.name
                if (code == 'ConditionalCheckFailedException') {
                    log.trace(`Update collision`, {err})
                } else if (code == 'ProvisionedThroughputExceededException') {
                    log.info(`Provisioned throughput exceeded: ${err.message}`, {err, cmd: command, item})
                } else {
                    log.error(`Emit exception code ${err.name} ${err.code} message ${err.message}`, {
                        err,
                        cmd: command,
                        item,
                    })
                    throw err
                }
                return false
            })(err, this.log)
        }
    }

    mapItemFromDB(data: any): Metric {
        let pk = data[this.primaryKey]
        let sk = data[this.sortKey]
        let owner = pk.split('#').pop()
        let [, namespace, metric, dimensions] = sk.split('#')
        let spans
        if (data.spans) {
            spans = data.spans.map((s) => {
                return {
                    end: s.se,
                    period: s.sp,
                    samples: s.ss,
                    points: s.pt.map((p) => {
                        let point = {count: Number(p.c), sum: Number(p.s)} as Point
                        if (p.x != null) {
                            point.max = Number(p.x)
                        }
                        if (p.m != null) {
                            point.min = Number(p.m)
                        }
                        if (p.v) {
                            point.pvalues = p.v
                        }
                        return point
                    }),
                }
            })
        }
        let expires = data[this.expires]
        let seq = data.seq
        return {dimensions, expires, metric, namespace, owner, seq, spans}
    }

    mapItemToDB(item: Metric) {
        let result = {
            [this.primaryKey]: `${this.prefix}#${Version}#${item.owner}`,
            [this.sortKey]: `${this.prefix}#${item.namespace}#${item.metric}#${item.dimensions}`,
            [this.expires]: item.expires,
            spans: item.spans.map((i) => {
                return {
                    se: i.end,
                    sp: i.period,
                    ss: i.samples,
                    pt: i.points.map((point) => {
                        let p = {c: point.count, s: this.round(point.sum)} as any
                        if (point.max != null) {
                            p.x = this.round(point.max)
                        }
                        if (point.min != null) {
                            p.m = this.round(point.min)
                        }
                        if (point.pvalues) {
                            p.v = point.pvalues
                        }
                        return p
                    }),
                }
            }),
            seq: item.seq,
            _source: item._source,
        }
        if (this.type) {
            let [key, model] = Object.entries(this.type)[0]
            result[key] = model
        }
        return result
    }

    formatDate(n: number) {
        function padTo2Digits(num: number) {
            return num.toString().padStart(2, '0')
        }
        let date = new Date(n)
        const year = date.getFullYear().toString().slice(-2) // Get last two digits of the year
        const month = padTo2Digits(date.getMonth() + 1) // Months are zero-indexed
        const day = padTo2Digits(date.getDate())
        const hours = padTo2Digits(date.getHours())
        const minutes = padTo2Digits(date.getMinutes())
        const seconds = padTo2Digits(date.getSeconds())
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
    }
    
    metricToString(metric: Metric): string {
        let buf: string[] = []
        buf.push(`${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions) || ''}`)
        for (let span of metric.spans) {
            let interval = span.period / span.samples
            let start = span.end - span.points.length * interval
            buf.push(
                ` ${span.period} secs ${this.formatDate(start * 1000)} => ` + 
                `${this.formatDate(span.end * 1000)} ${span.points.length} points`
            )
            for (let point of span.points) {
                buf.push(`     count ${point.count} = sum ${point.sum}`)
            }
        }
        return buf.join('\n')
    }
    
    queryToString(metric: MetricQueryResult): string {
        let points = metric.points.slice(0)
        let buf: string[] = []
        buf.push(
            `${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions)} ${metric.period} ${
                points.length
            } points`
        )
        for (let point of points) {
            buf.push(`     ${this.formatDate(point.timestamp || 0)} = ${point.value || '-'} / ${point.count}`)
        }
        return buf.join('\n')
    }

    static freeInstanceByKey(key: string) {
        delete Instances[key]
    }

    static saveInstance(tags: object, metrics: CustomMetrics) {
        let key = JSON.stringify(tags)
        Instances[key] = metrics
    }

    /*
        Align a timestamp that is >timestamp and rounded up in seconds to be interval aligned
        Note: this may be in the future
     */
    private alignTime(span: Span, timestamp: number): number {
        let interval = span.period / span.samples
        return Math.ceil(timestamp / interval) * interval
    }

    /* istanbul ignore next */
    private assert(c: any) {
        if (!c && Assert) {
            let msg = {stack: ''}
            if (typeof Error.captureStackTrace === 'function') {
                Error.captureStackTrace(msg)
            } else {
                msg.stack = new Error('Assert').stack
            }
            this.log.error(`Assertion failed`, {stack: msg.stack})
        }
    }

    /* istanbul ignore next */
    private info(message: string, context = {}) {
        console.log('INFO: ' + message, context)
    }

    /* istanbul ignore next */
    private error(message: string, context = {}) {
        console.log('ERROR: ' + message, context)
    }

    /* istanbul ignore next */
    private trace(message: string, context = {}) {
        console.log('TRACE: ' + message, context)
    }

    /*
        Overcome Javascript number precision issues
        Round to 16 significant digits
     */
    round(n: number): number {
        /* istanbul ignore next */
        if (isNaN(n) || n == null) {
            return 0
        }
        let places = 16 - n.toFixed(0).length
        return Number(n.toFixed(places)) - 0
    }

    /* istanbul ignore next */
    jitter(msecs: number): number {
        return Math.min(10 * 1000, Math.floor(msecs / 2 + msecs * Math.random()))
    }

    /* istanbul ignore next */
    async delay(time: number): Promise<boolean> {
        return new Promise(function (resolve, reject) {
            setTimeout(() => resolve(true), time)
        })
    }
}

//  Emulate the SenseLogs logger
/* istanbul ignore next */
class Log {
    private senselogs = null
    private logger = null
    private verbose = false
    constructor(dest: boolean | any) {
        if (dest === true) {
            this.logger = this.defaultLogger
        } else if (dest == 'verbose') {
            this.logger = this.defaultLogger
            this.verbose = true
        } else if (dest && typeof dest.info == 'function') {
            this.senselogs = dest
        }
    }

    error(message: string, context: string) {
        this.process('error', message, context)
    }

    info(message: string, context: string) {
        this.process('info', message, context)
    }

    trace(message: string, context: string) {
        this.process('trace', message, context)
    }

    process(chan: string, message: string, context: string) {
        if (this.logger) {
            this.logger(chan, message, context)
        } else if (this.senselogs) {
            this.senselogs[chan](message, context)
        }
    }

    /* istanbul ignore next */
    defaultLogger(chan: string, message: string, context: object) {
        if (chan == 'trace' && !this.verbose) {
            //  params.log: true will cause the chan to be changed to 'info'
            return
        }
        let tag = chan.toUpperCase()
        if (context) {
            try {
                console.log(tag, message, JSON.stringify(context, null, 4))
            } catch (err) {
                let buf = ['{']
                for (let [key, value] of Object.entries(context)) {
                    try {
                        buf.push(`    ${key}: ${JSON.stringify(value, null, 4)}`)
                    } catch (err) {
                        /* continue */
                    }
                }
                buf.push('}')
                console.log(tag, message, buf.join('\n'))
            }
        } else {
            console.log(tag, message)
        }
    }
}
