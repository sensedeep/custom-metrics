/*
    CustomMetrics - Simple, configurable, economical metrics for AWS
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
const Assert = true
const Buffering = true
const DefaultResolution = 0 // Default number of p-values to store
const MaxSeq = Number.MAX_SAFE_INTEGER // Maximum sequence number for collision detection
const MaxRetries = 10 // Max retries when emitting a metric and encountering collisions
const MetricListLimit = 10000

type SpanDef = {
    period: number // Span's total timespan
    samples: number // Number of data points in period
}

/*
    Default spans are configurable by the constructor
 */
export const DefaultSpans: SpanDef[] = [
    {period: 5 * 60, samples: 10}, //  5 mins, interval: 30 secs
    {period: 60 * 60, samples: 12}, //  1 hr, interval: 5 mins
    {period: 24 * 60 * 60, samples: 12}, //  24 hrs, interval: 2 hrs
    {period: 7 * 24 * 60 * 60, samples: 14}, //  7 days, interval: 1/2 day
    {period: 28 * 24 * 60 * 60, samples: 14}, //  28 days, interval: 2 days
    {period: 365 * 24 * 60 * 60, samples: 12}, //  1 year, interval: 1 month
]

export type Metric = {
    dimensions: string
    expires?: number
    id?: string
    metric: string
    namespace: string
    owner?: string
    version?: number
    spans: Span[]
    seq?: number
    //  Useful when using streams to filter out items with this attribute
    _source?: string
}

export type Point = {
    count: number
    max?: number
    min?: number
    pvalues?: number[]
    sum: number
    //  Never stored
    timestamp?: number
}

export type Span = {
    end: number
    period: number
    samples: number
    points: Point[]
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
    max?: number
    min?: number
    pvalues?: number[]
    sum?: number
    timestamp?: number
    value?: number
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
    log?: true | 'verbose' | any
    owner?: string
    primaryKey?: string
    sortKey?: string
    prefix?: string
    pResolution?: number
    region?: string
    source?: string
    spans?: SpanDef[]
    table?: string
    ttl?: number
    type?: {[key: string]: string} // Optional single type type field
    // DEPRECATE
    tableName?: string
}

export type MetricBufferOptions = {
    sum?: number
    count?: number
    elapsed?: number // Elapsed time in seconds
    force?: boolean
}

export type MetricEmitOptions = {
    buffer?: MetricBufferOptions
    log?: boolean
    owner?: string
    timestamp?: number
    ttl?: number
}

export type MetricListOptions = {
    log?: boolean
    limit?: number
    owner?: string
}

export type MetricQueryOptions = {
    accumulate?: boolean
    id?: string
    log?: boolean
    owner?: string
    timestamp?: number
}

type BufferElt = {
    count: number
    dimensions: string
    metric: string
    namespace: string
    sum: number
    timestamp: number
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
    private buffers: BufferMap = {}
    private client: DynamoDBClient
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
    private timestamp: number
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
            }
            if (options.region) {
                //  Allow region in credentials
                params.region = (params.credentials as any).region || options.region
            }
            this.client = new DynamoDBClient(params)
        }
        if (!options.table && !options.tableName) {
            throw new Error('Missing DynamoDB table name property')
        }
        //  DEPRECATE tableName
        /* istanbul ignore next */
        this.table = options.table || options.tableName

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
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000)
        let point: Point
        point = {count: 1, sum: value}
        return await this.emitDimensions(namespace, metricName, point, dimensionsList, options)
    }

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
            if (buffer && Buffering) {
                result = await this.bufferMetric(namespace, metricName, point, dimensions, options)
            } else {
                result = await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options)
            }
        }
        return result!
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
        let ttl = options.ttl != undefined ? options.ttl : this.ttl
        let retries = MaxRetries
        let metric: Metric | undefined
        let backoff = 10
        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        do {
            let owner = options.owner || this.owner
            metric = await this.getMetric(owner, namespace, metricName, dimensions)
            if (!metric) {
                metric = this.initMetric(owner, namespace, metricName, dimensions)
            }
            if (point.timestamp) {
                /*
                    point.timestamp will be set for buffered metrics which may be in the past
                */
                let si = metric.spans.findIndex((s) => s.end - s.period <= point.timestamp && point.timestamp < s.end)
                /* istanbul ignore else */
                if (si >= 0) {
                    this.addValue(metric, point.timestamp, point, si)
                } else {
                    this.log.error('Cannot determine span index', {point, metric})
                }
            } else {
                this.addValue(metric, point.timestamp, point, 0)
            }
            /* istanbul ignore next */
            if (this.source) {
                metric._source = this.source
            }
            /*
                Set the expiration TTL. Defaults to the longest span.
                Users may define a shorter TTL to prune metrics for inactive items.
            */
            if (ttl) {
                metric.expires = this.timestamp + ttl
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
        Buffer a metric for specific dimensions
     */
    async bufferMetric(
        namespace: string,
        metricName: string,
        point: Point,
        dimensions: string,
        options: MetricEmitOptions
    ): Promise<Metric> {
        let buffer = options.buffer || this.buffer
        let interval = this.spans[0].period / this.spans[0].samples
        let key = `${namespace}|${metricName}|${JSON.stringify(dimensions)}`
        let elt: BufferElt = (this.buffers[key] = this.buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: this.timestamp + (buffer.elapsed || interval),
            namespace: namespace,
            metric: metricName,
            dimensions,
        })
        if (
            buffer.force ||
            (buffer.sum && elt.sum >= buffer.sum) ||
            (buffer.count && elt.count >= buffer.count) ||
            this.timestamp >= elt.timestamp
        ) {
            /* KEEP
            this.log.trace(
                `Emit buffered metric ${namespace}/${metricName} = ${point.sum}, sum ${elt.sum} count ${
                    elt.count
                } remaining ${elt.timestamp - this.timestamp}`
            ) */
            point.timestamp = elt.timestamp
            await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options)
        }
        elt.count += point.count
        elt.sum += point.sum

        /* KEEP
        this.log.trace(
            `Buffer metric ${namespace}/${metricName} = sum ${elt.sum} count ${elt.count}, remaining ${
                elt.timestamp - this.timestamp
            }`, {buffers: this.buffers}
        ) */

        CustomMetrics.saveInstance({key}, this)
        return {
            spans: [{points: [{count: elt.count, sum: elt.sum}]}],
            metric: metricName,
            namespace: namespace,
            owner: options.owner || this.owner,
            version: Version,
        } as Metric
    }

    /*
        Flush metrics for all instances on Lambda termination
     */
    static async terminate() {
        // let start = Date.now()
        await CustomMetrics.flushAll()
        // console.log(`Lambda terminating, metric flush took ${(Date.now() - start) / 1000} ms`)
    }

    static async flushAll() {
        for (let [key, instance] of Object.entries(Instances)) {
            await instance.flush()
            CustomMetrics.freeInstanceByKey(key)
        }
        Instances = {}
    }

    async flush() {
        for (let elt of Object.values(this.buffers)) {
            let point = {count: elt.count, sum: elt.sum}
            await this.emitDimensionedMetric(elt.namespace, elt.metric, point, elt.dimensions)
        }
        this.buffers = {}
    }

    /*
        Query metrics. Return an array of metrics
     */
    async query(
        namespace: string,
        metricName: string,
        dimensions: MetricDimensions,
        period: number,
        statistic: string,
        options: MetricQueryOptions = {}
    ): Promise<MetricQueryResult> {
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000)

        let owner = options.owner || this.owner

        /*
           Flush buffered metrics for this instance. Will still not see buffered metrics in
           other instances until they are flushed.
         */
        await this.flush()

        /*
            Get metrics for all spans up to and including the desired span
         */
        let dimString = this.makeDimensionString(dimensions)

        let metric = await this.getMetric(owner, namespace, metricName, dimString)
        if (!metric) {
            return {dimensions, id: options.id, metric: metricName, namespace, period, points: [], owner, samples: 0}
        }
        /*
            Map the period to the closest span that has a period equal or larger.
            If the period is too big, then use the largest span period.
         */
        let span = metric.spans.find((s) => period <= s.period)
        if (!span) {
            span = metric.spans[metric.spans.length - 1]
            period = span.period
        }
        /*
            Aggregate data for all spans less than the desired span.
            Do this because spans are updated lazily on emit.
         */
        this.addValue(metric, this.timestamp, {count: 0, sum: 0}, 0, period)

        let result: MetricQueryResult

        /* istanbul ignore else */
        if (metric && span) {
            if (options.accumulate) {
                result = this.accumulateMetric(metric, span, statistic, owner)
            } else {
                result = this.calculateSeries(metric, span, statistic, owner)
            }
        } else {
            //  Should never happen as spans are created for the desired period
            result = {dimensions, metric: metricName, namespace, period, points: [], owner, samples: span.samples}
        }
        result.id = options.id
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

    /*
        Accumulate the metric data points into a single value. 
        This is useful for gauges and widgets that need a single data value for the metric.
        According to the desired statistic, this calculates the avg, count, max, min, sum, and pvalues.
     */
    private accumulateMetric(metric: Metric, span: Span, statistic: string, owner: string): MetricQueryResult {
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
        } else if (statistic.match(/^p[0-9]+/)) {
            pvalues = []
        } /* avg */ else {
            value = 0
            count = 0
        }
        let points = span.points
        for (let i = 0; i < points.length; i++) {
            let point = points[i]
            if (statistic == 'max') {
                if (point.max != undefined) {
                    value = Math.max(value, point.max)
                }
            } else if (statistic == 'min') {
                if (point.min != undefined) {
                    value = Math.min(value, point.min)
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
        if (statistic.match(/^p[0-9]+/)) {
            let p = parseInt(statistic.slice(1))
            pvalues.sort((a, b) => a - b)
            let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
            value = pvalues[nth]
        } else if (statistic == 'avg') {
            value /= Math.max(count, 1)
        }
        /* istanbul ignore next */
        let timestamp = (this.timestamp || Date.now()) * 1000
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{value, timestamp, count}],
            samples: span.samples,
        }
    }

    /*
        Process the metric points. This is used for graphs that need all the data points.
        This calculates the avg, max, min, sum, count, pvalues and per-point timestamps.
     */
    private calculateSeries(metric: Metric, span: Span, statistic: string, owner: string): MetricQueryResult {
        let points: MetricQueryPoint[] = []
        let interval = span.period / span.samples

        /*
            The point timestamp for the point is the end of the point, not the start, so -1
            Also, the last point may have an end beyond the current time, so limit.
         */
        let timestamp = span.end - span.points.length * interval
        let value: number = undefined
        let i = 0
        for (let point of span.points) {
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
                } /* avg */ else {
                    value = point.sum / point.count
                }
            } else {
                value = 0
            }
            //  Want timestamp to be the end of the point bucket
            timestamp += interval
            timestamp = Math.min(timestamp, this.timestamp)
            points.push({value, count: point.count, timestamp: timestamp * 1000})
            i++
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
        Add a point value to the metric at a desired timestamp (and/or span index).
        We aggregate aged out points to upper spans as required.
        For queries, a queryPeriod nominates the desired span, all lower span values are aggregated up.
     */
    private addValue(
        metric: Metric,
        timestamp: number = this.timestamp,
        point: Point,
        si: number,
        queryPeriod: number = 0
    ) {
        this.assert(metric)
        this.assert(timestamp)
        this.assert(0 <= si && si < metric.spans.length)

        let span = metric.spans[si]
        let interval = span.period / span.samples
        /* istanbul ignore next */
        let points = span.points || []
        let start = span.end - points.length * interval

        //  Aggregate points to higher spans if not querying or if querying and not yet at desired period
        let aggregate = !queryPeriod || span.period < queryPeriod ? true : false

        //  Just for safety
        /* istanbul ignore next */
        while (points.length > span.samples) {
            points.shift()
        }

        /*
            Aggregate points. Calculate how many points have aged, or if querying, how many must be aggregated.
        */
        if (points.length) {
            /* istanbul ignore next */
            if (timestamp < start) {
                this.log.error('Bad metric span', {metric, point, timestamp, start, span})
                return
            }
            let shift = 0
            if (queryPeriod && aggregate) {
                //  Querying and not yet on the target period, so aggregate all points
                shift = points.length
            } else if (point.count) {
                //  Point to add and either querying and not on the target period or normal emit.
                //  Add one more to make room for this point being added incase points[] is full.
                shift = Math.floor((timestamp - start) / interval) - span.samples + 1
            } else if (queryPeriod) {
                //  Querying and on target period so, shift out all points that have aged out.
                shift = Math.floor((timestamp - start) / interval) - span.samples
            }
            shift = Math.max(0, Math.min(shift, points.length))
            this.assert(0 <= shift && shift <= points.length)

            /*
                Shift out aged points to make room. Propagate up to higher spans.
             */
            while (shift-- > 0) {
                let p = points.shift()
                //  Recurse and add the point to the next metric. If querying, only to this if recursing
                if (aggregate && p.count && si + 1 < metric.spans.length) {
                    this.addValue(metric, start, p, si + 1, queryPeriod)
                }
                start += interval
            }
        }
        if (aggregate && queryPeriod && si + 1 < metric.spans.length) {
            //  Querying and must recurse to aggregate all periods up to the target
            this.addValue(metric, timestamp, point, si + 1, queryPeriod)
        } else if (point.count) {
            if (points.length == 0) {
                start = span.end = this.getTimestamp(span, timestamp)
            }
            //  Desired time period pre-dates span points
            while (timestamp < start) {
                points.unshift({count: 0, sum: 0})
                start -= interval
            }
            //  Must always push one point space for the current point
            while (timestamp >= span.end) {
                points.push({count: 0, sum: 0})
                span.end += interval
            }
            this.assert(points.length <= span.samples)

            let index = Math.floor((timestamp - start) / interval)
            this.assert(0 <= index && index < points.length)
            this.setPoint(span, index, point)
        }
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
        let next: object | undefined
        let limit = options.limit || MetricListLimit
        /* istanbul ignore next */
        let chan = options.log == true ? 'info' : 'trace'
        let items, command
        do {
            /* istanbul ignore next */
            ;({command, items, next} = await this.findMetrics(owner, namespace, metric, limit, next))
            this.log[chan](`Find metrics ${namespace}, ${metric}`, {command, items})
            if (items.length) {
                for (let item of items) {
                    let ns = (map[item.namespace] = map[item.namespace] || {})
                    let met = (ns[item.metric] = ns[item.metric] || [])
                    met.push(item.dimensions)
                }
            }
        } while (next)

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

    private initMetric(owner: string, namespace: string, name: string, dimensions: string): Metric {
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
                end: this.timestamp,
                points: [],
            }
            span.end = this.getTimestamp(span)
            metric.spans.push(span)
        }
        return metric
    }

    async getMetric(owner: string, namespace: string, metric: string, dimensions: string): Promise<Metric> {
        let command = new GetItemCommand({
            TableName: this.table,
            Key: {
                [this.primaryKey]: {S: `${this.prefix}#${Version}#${owner}`},
                [this.sortKey]: {S: `${this.prefix}#${namespace}#${metric}#${dimensions}`},
            },
            ConsistentRead: this.consistent,
        })
        let data = await this.client.send(command)
        if (data.Item) {
            let item = unmarshall(data.Item)
            return this.mapItemFromDB(item)
        }
        return null
    }

    async findMetrics(
        owner: string,
        namespace: string,
        metric: string | undefined,
        limit: number,
        startKey: object
    ): Promise<{items: Metric[]; next: object; command: QueryCommand}> {
        let key = [namespace]
        if (metric) {
            key.push(metric)
        }
        /* istanbul ignore next */

        let start = startKey ? marshall(startKey) : undefined
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
            ProjectionExpression: `${this.primaryKey}, ${this.sortKey}`,
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
        this.log[chan](`Put metric ${item.namespace}, ${item.metric}`, {dimensions: item.dimensions, command})

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
        let expires = data.expires
        let seq = data.seq
        return {dimensions, expires, metric, namespace, owner, seq, spans}
    }

    mapItemToDB(item: Metric) {
        let result = {
            [this.primaryKey]: `${this.prefix}#${Version}#${item.owner}`,
            [this.sortKey]: `${this.prefix}#${item.namespace}#${item.metric}#${item.dimensions}`,
            expires: item.expires,
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
        // TODO REMOVE assert
        for (let span of result.spans) {
            for (let p of span.pt) {
                this.assert(p.s != null && !isNaN(p.s))
            }
        }
        return result
    }

    /*
        UNDOCUMENTED DEPRECATED
        Allocate a CustomMetrics instance from the cache
     */
    static allocInstance(tags: object, options: MetricOptions = {}) {
        let key = JSON.stringify(tags)
        let metrics = Instances[key]
        if (!metrics) {
            metrics = Instances[key] = new CustomMetrics(options)
        }
        return metrics
    }

    static freeInstance(tags: object) {
        let key = JSON.stringify(tags)
        delete Instances[key]
    }

    static freeInstanceByKey(key: string) {
        delete Instances[key]
    }

    static getInstance(tags: object) {
        let key = JSON.stringify(tags)
        return Instances[key]
    }

    static saveInstance(tags: object, metrics: CustomMetrics) {
        let key = JSON.stringify(tags)
        Instances[key] = metrics
    }

    static getCache() {
        return Instances
    }

    /*
        Get a rounded timestamp in seconds
     */
    private getTimestamp(span: Span, timestamp: number = this.timestamp): number {
        let interval = span.period / span.samples
        return Math.ceil(timestamp / interval) * interval
    }

    /* istanbul ignore next */
    private assert(c) {
        if (!c && Assert) {
            let msg = {stack: ''}
            if (typeof Error.captureStackTrace === 'function') {
                Error.captureStackTrace(msg)
            } else {
                msg.stack = new Error('Assert').stack
            }
            this.log.error(`Assertion failed ${msg.stack}`)
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

    //  Overcome Javascript number precision issues
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

    /* KEEP
    Fill a query result with zero data points for all missing points

    function fill(metrics) {
        for (let metric of metrics) {
            let nextTime = metric.start.getTime()
            let end = metric.end.getTime()
            let interval = (metric.period / metric.samples) * 1000
            let points = []
            for (let point of metric.points) {
                while (nextTime + interval < point.timestamp) {
                    nextTime += interval
                    points.push({timestamp: nextTime, value: 0, count: 0})
                }
                points.push(point)
                nextTime = point.timestamp
            }
            nextTime += interval

            while (nextTime < end) {
                points.push({timestamp: nextTime, value: 0, count: 0})
                nextTime += interval
            }
            metric.points = points
        }
    } */
}