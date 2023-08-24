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
    creds?: DynamoDBClientConfig
    log?: true | 'verbose' | any
    owner?: string
    primaryKey?: string
    sortKey?: string
    prefix?: string
    pResolution?: number
    source?: string
    spans?: SpanDef[]
    table?: string
    // DEPRECATE
    tableName?: string
    typeField?: string
    ttl?: number
}

export type MetricBufferOptions = {
    sum?: number
    count?: number
    elapsed?: number // Elapsed time in seconds
    force?: boolean
}

export type MetricEmitOptions = {
    buffer?: MetricBufferOptions
    owner?: string
    timestamp?: number
    ttl?: number
}

export type MetricListOptions = {
    log?: boolean
    limit?: number
    // next?: object
    owner?: string
}

export type MetricQueryOptions = {
    accumulate?: boolean
    id?: string
    owner?: string
    timestamp?: number
}

type BufferElt = {
    count: number
    dimensions: MetricDimensionsList
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
    On exit, flush any buffered metrics
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
    // private MetricModel: Model<Metric>
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
    private ttl: number

    constructor(options: MetricOptions = {}) {
        if (options.log == true) {
            this.log = {info: this.nop, error: this.error}
        } else if (options.log == 'verbose') {
            this.log = {info: this.info, error: this.error}
        } else if (options.log) {
            this.log = options.log
        } else {
            this.log = {info: this.nop, error: this.nop}
        }
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

        if (options.client) {
            this.client = options.client
        } else {
            this.client = new DynamoDBClient(options.creds || {})
        }
        if (!options.table && !options.tableName) {
            throw new Error('Missing DynamoDB table name property')
        }
        //  DEPRECATE tableName
        /* istanbul ignore next */
        this.table = options.table || options.tableName
        
        this.options = options
        this.owner = options.owner || 'account'
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
        if (dimensionsList.length == 0) {
            dimensionsList = [{}]
        }
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000)
        let point: Point
        let buffer = options.buffer || this.buffer
        if (buffer && Buffering) {
            return await this.bufferMetric(namespace, metricName, value, dimensionsList, options)
        }
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
        for (let dimensions of dimensionsList) {
            let dimString = this.makeDimensionString(dimensions)
            result = await this.emitDimensionedMetric(namespace, metricName, point, dimString, options)
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
        do {
            let owner = options.owner || this.owner
            metric = await this.getMetric(owner, namespace, metricName, dimensions)
            if (!metric) {
                this.log.info(`Initializing new metric`, {namespace, metricName, dimensions, owner})
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
            try {
                await this.updateMetric(metric, point, ttl)
                break
            } catch (err: any) {
                /* istanbul ignore next */
                if (err.code != 'ConditionalCheckFailedException') {
                    this.log.info(`Emit exception code ${err.code} message ${err.message}`, err)
                    throw err
                }
            }
            /* istanbul ignore next */
            if (retries == 0) {
                this.log.error(`Metric has too many retries`, {namespace, metricName, dimensions})
                break
            }
            /* istanbul ignore next */
            this.log.info(`Retry metric update`, {retries})
        } while (retries-- > 0)
        return metric
    }

    async bufferMetric(
        namespace: string,
        metricName: string,
        value: number,
        dimensionsList: MetricDimensionsList,
        options: MetricEmitOptions
    ): Promise<Metric> {
        let buffer = options.buffer || this.buffer
        let interval = this.spans[0].period / this.spans[0].samples
        let key = `${namespace}|${metricName}|${JSON.stringify(dimensionsList)}`
        let elt: BufferElt = (this.buffers[key] = this.buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: this.timestamp + (buffer.elapsed || interval),
            namespace: namespace,
            metric: metricName,
            dimensions: dimensionsList,
        })
        if (
            buffer.force ||
            (buffer.sum && elt.sum >= buffer.sum) ||
            (buffer.count && elt.count >= buffer.count) ||
            this.timestamp >= elt.timestamp
        ) {
            //  Remove this trace soon
            this.log.info(
                `Emit buffered metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${
                    elt.count
                } remaining ${elt.timestamp - this.timestamp}`
            )
            let point = {count: elt.count, sum: elt.sum, timestamp: elt.timestamp}
            await this.emitDimensions(namespace, metricName, point, dimensionsList, options)
        }
        elt.count++
        elt.sum += value

        //  Remove this trace soon
        this.log.info(
            `Buffer metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${elt.count}, remaining ${
                elt.timestamp - this.timestamp
            }`
        )
        CustomMetrics.saveInstance({key}, this)
        return {
            spans: [{points: [{count: elt.count, sum: elt.sum}]}],
            metric: metricName,
            namespace: namespace,
            owner: options.owner || this.owner,
            version: Version,
        } as Metric
    }

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
            for (let dimensions of elt.dimensions) {
                await this.emitDimensionedMetric(elt.namespace, elt.metric, point, this.makeDimensionString(dimensions))
            }
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

        this.log.info(`Query metrics ${namespace}/${metricName}`, {dimensions})
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
            return {dimensions, metric: metricName, namespace, period, points: [], owner, samples: 0}
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
        // this.log.info(`@@ QUERY RETURN`, {result})
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
        Convert a dimensions object of the form {key: value, ...} to a string with comma separated "key=value,..."
     */
    private makeDimensionString(dimensions: MetricDimensions): string {
        let result: string[] = []
        for (let [name, value] of Object.entries(dimensions)) {
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
        Save metric to disk. Point is provided just for logging
     */
    private async updateMetric(metric: Metric, point: Point, ttl: number) {
        /* istanbul ignore next */
        if (this.source) {
            metric._source = this.source
        }
        /*
            Set the expiration TTL. Defaults to the longest span.
            Users may define a shorter TTL to prune metrics for inactive items.
            WARNING: these are in seconds
        */
        if (ttl) {
            metric.expires = (this.timestamp + ttl) * 1000
        }
        await this.putMetric(metric)
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
        let items
        do {
            /* istanbul ignore next */
            ;({items, next} = await this.findMetrics(owner, namespace, metric, limit, next))
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
        // this.log.info(`@@ GET METRIC`, {command})
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
    ): Promise<{items: Metric[]; next: object}> {
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
        // this.log.info(`@@ LIST COMMAND`, {command})
        let result = await this.client.send(command)
        // this.log.info(`@@ LIST RETURN`, {result})
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
        return {items, next}
    }

    /*
        Use a sequence number to detect simultaneous updated. If collision, will throw 
        a ConditionalCheckFailedException and emit() will then retry.
    */
    async putMetric(item: Metric) {
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
            ExpressionAttributeValues = { ':_0': {N: seq.toString()} }
        } else {
            item.seq = 0
        }
        let mapped = this.mapItemToDB(item)
        let params: PutItemCommandInput = {
            TableName: this.table,
            ReturnValues: 'NONE',
            Item : marshall(mapped, {removeUndefinedValues: true}),
            ConditionExpression,
            ExpressionAttributeValues,
        }
        let command = new PutItemCommand(params)
        this.log.info(`@@@ PUT METRIC`, {command})
        return await this.client.send(command)
    }

    mapItemFromDB(data: any): Metric {
        /*
        for (let [key, items] of Object.entries(responses)) {
            for (let item of items) {
        */
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
                        return {
                            count: p.c,
                            max: p.x,
                            min: p.m,
                            pvalues: p.v,
                            sum: p.s,
                        }
                    }),
                }
            })
        }
        let expires = data.expires
        let seq = data.seq
        return {dimensions, expires, metric, namespace, owner, seq, spans}
    }

    mapItemToDB(item: Metric) {
        return {
            [this.primaryKey]: `${this.prefix}#${Version}#${item.owner}`,
            [this.sortKey]: `${this.prefix}#${item.namespace}#${item.metric}#${item.dimensions}`,
            expires: Math.ceil(item.expires),
            spans: item.spans.map((i) => {
                return {
                    se: i.end,
                    sp: i.period,
                    ss: i.samples,
                    pt: i.points.map((p) => {
                        return {
                            c: p.count,
                            x: p.max,
                            m: p.min,
                            s: p.sum,
                            v: p.pvalues,
                        }
                    }),
                }
            }),
            seq: item.seq,
            _source: item._source,
        }
    }

    /*
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

    /* KEEP
    getPValue(point: Point, p: number): number {
        let pvalues = point.pvalues.sort()
        let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
        return point.pvalues[nth]
    } */

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
    private nop() {}
}
