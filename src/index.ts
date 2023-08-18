/*
    CustomMetrics - Simple, configurable, economical metrics for AWS
 */

import process from 'process'
import {Schema, Version} from './schema'
import {Entity, Model, Table} from 'dynamodb-onetable'

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
    metric: string
    namespace: string
    owner: string
    period: number
    points: MetricQueryPoint[]
}

// Constructor options
export type MetricOptions = {
    buffer?: MetricBufferOptions
    client?: object
    log?: true | 'verbose' | any
    onetable?: Table
    owner?: string
    primaryKey?: string
    sortKey?: string
    prefix?: string
    pResolution?: number
    source?: string
    spans?: SpanDef[]
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
    fields?: string[]
    log?: boolean
    limit?: number
    next?: object
    owner?: string
}

export type MetricQueryOptions = {
    accumulate?: boolean
    owner?: string
    timestamp?: number
}

export type Span = Entity<typeof Schema.models.Metric.spans.items.schema>
export type Point = Entity<typeof Schema.models.Metric.spans.items.schema.points.items.schema>

export type Metric = Entity<typeof Schema.models.Metric>

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
process.on('SIGTERM', async () => {
    await CustomMetrics.terminate()
})

export class CustomMetrics {
    private buffer: MetricBufferOptions | undefined
    private buffers: BufferMap = {}
    private db: Table
    private log: any
    private MetricModel: Model<Metric>
    private options: MetricOptions
    private owner: string
    private pResolution: number
    private source: string | undefined
    private spans: SpanDef[]
    private timestamp: number
    private ttl: number

    constructor(options: MetricOptions = {}) {
        if (options.log == true) {
            this.log = {
                info: (message: string, context: {}) => null,
                error: (message: string, context: {}) => console.log('ERROR: ' + message, context),
            }
        } else if (options.log == 'verbose') {
            this.log.info = (message: string, context: {}) => console.log('INFO: ' + message, context)
        } else if (options.log) {
            this.log = options.log
        } else {
            this.log = {
                info: (message: string, context: {}) => null,
                error: (message: string, context: {}) => null,
            }
        }
        if (!options.owner) {
            throw new Error('Missing required "owner" in options')
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
        if (options.prefix) {
            let model = Schema.models.Metric as any
            model.pk.value = model.pk.value.replace('metric', options.prefix)
            model.sk.value = model.sk.value.replace('metric', options.prefix)
        }
        if (options.buffer) {
            if (typeof options.buffer != 'object') {
                throw new Error('Bad type for "buffer" option')
            }
            this.buffer = options.buffer
        }
        if (options.onetable) {
            this.db = options.onetable
            try {
                this.MetricModel = this.db.getModel('Metric')
            } catch (err) {
                this.db.addModel('Metric', Schema.models.Metric)
                this.MetricModel = this.db.getModel('Metric')
            }
        } else {
            if (!options.client) {
                throw new Error('Missing AWS V3 SDK DynamoDB client instance')
            }
            if (!options.tableName) {
                throw new Error('Missing DynamoDB tableName property')
            }
            let schema = Object.assign({}, Schema)
            schema.indexes.primary.hash = options.primaryKey || 'pk'
            schema.indexes.primary.sort = options.sortKey || 'sk'
            schema.params.typeField = options.typeField || '_type'
            this.db = new Table({
                client: options.client,
                hidden: false,
                name: options.tableName,
                partial: true,
                schema,
            })
            this.MetricModel = this.db.getModel('Metric')
        }
        this.options = options
        this.owner = options.owner || 'account'
        this.spans = options.spans || DefaultSpans
        this.ttl = options.ttl || this.spans[this.spans.length - 1].period

        if (this.options.source) {
            this.source = this.options.source
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
            point = this.bufferMetric(namespace, metricName, value, dimensionsList, buffer)
            if (point.timestamp === 0) {
                //  Buffer signal. Return a partial metric with only the buffered point.
                let result = {
                    spans: [{points: [point]}],
                    metric: metricName,
                    namespace: namespace,
                    owner: options.owner || this.owner,
                    version: Version,
                }
                return result as Metric
            }
        } else {
            point = {count: 1, sum: value}
        }
        let result: Metric
        for (let dimensions of dimensionsList) {
            let dimString = this.makeDimensionString(dimensions)
            result = await this.emitDimensionMetric(namespace, metricName, point, dimString, options)
        }
        return result!
    }

    /*
        Emit a metric for specific dimensions
     */
    private async emitDimensionMetric(
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
            metric = await this.MetricModel.get(
                {owner, namespace, metric: metricName, dimensions: dimensions, version: Version},
                {hidden: true}
            )
            if (!metric) {
                this.log.info(`Initializing new metric`, {namespace, metricName, dimensions, owner})
                metric = this.initMetric(owner, namespace, metricName, dimensions)
            }
            if (point.timestamp) {
                /*
                    point.timestamp will be set for buffered metrics which may be in the past
                */
                let si = metric.spans.findIndex((s) => s.end - s.period <= point.timestamp && point.timestamp < s.end)
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
                if (err.code != 'ConditionalCheckFailedException') {
                    this.log.info(`Emit exception code ${err.code} message ${err.message}`, err)
                    throw err
                }
            }
            if (retries == 0) {
                this.log.error(`Metric has too many retries`, {namespace, metricName, dimensions})
                break
            }
            this.log.info(`Retry metric update`, {retries})
        } while (retries-- > 0)
        return metric
    }

    bufferMetric(
        namespace: string,
        metricName: string,
        value: number,
        dimensionsList: MetricDimensionsList,
        options: MetricBufferOptions
    ): Point {
        let interval = this.spans[0].period / this.spans[0].samples
        let key = `${namespace}|${metricName}|${JSON.stringify(dimensionsList)}`
        let elt: BufferElt = (this.buffers[key] = this.buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: this.timestamp + (options.elapsed || interval),
            namespace: namespace,
            metric: metricName,
            dimensions: dimensionsList,
        })
        elt.sum += value
        elt.count++
        if (
            options.force ||
            (options.sum && elt.sum >= options.sum) ||
            (options.count && elt.count >= options.count) ||
            this.timestamp >= elt.timestamp
        ) {
            //  Emit buffered values now
            delete this.buffers[key]
            this.log.info(
                `Emit buffered metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${
                    elt.count
                } remaining ${elt.timestamp - this.timestamp}`,
                {elt, buffers: this.buffers}
            )
            return {count: elt.count, sum: elt.sum, timestamp: elt.timestamp}
        }
        CustomMetrics.saveInstance({key}, this)
        this.log.info(
            `Buffer metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${elt.count}, remaining ${
                elt.timestamp - this.timestamp
            }`
        )
        //  Buffer and don't emit. Use timestamp == 0 as a signal to not emit
        return {count: elt.count, sum: elt.sum, timestamp: 0}
    }

    static async terminate() {
        let start = Date.now()
        await CustomMetrics.flushAll()
        console.log(`Lambda terminating, metric flush took ${(Date.now() - start) / 1000} ms`)
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
                await this.emitDimensionMetric(elt.namespace, elt.metric, point, this.makeDimensionString(dimensions))
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

        let metric = await this.MetricModel.get({
            owner,
            namespace,
            metric: metricName,
            dimensions: dimString,
            version: Version,
        })
        if (!metric) {
            return {dimensions, metric: metricName, namespace, period, points: [], owner}
        }
        /*
            Map the period to the closest span that has a period equal or larger.
            If the period is too big, then use the largest span period.
         */
        let span = metric.spans.find((s) => period <= s.period)
        if (!span) {
            span = metric.spans.at(-1)
            period = span.period
        }
        this.log.info(`Query ${namespace} ${metricName} ${dimString} ${period} ${statistic}`, {
            owner,
            metric,
            accumulate: options.accumulate,
            dimensions,
            period,
        })

        /*
            Aggregate data for all spans less than the desired span.
            Do this because spans are updated lazily on emit.
         */
        this.addValue(metric, this.timestamp, {count: 0, sum: 0}, 0, period)

        let result: MetricQueryResult
        if (metric && span) {
            if (options.accumulate) {
                result = this.accumulateMetric(metric, span, statistic, owner)
            } else {
                result = this.calculateSeries(metric, span, statistic, owner)
            }
        } else {
            result = {dimensions, metric: metricName, namespace, period, points: [], owner}
        }
        this.log.info(
            `Metric query ${namespace}, ${metricName}, ${this.makeDimensionString(dimensions)}, ` +
                `period ${period}, statistic "${statistic}"`,
            {result}
        )
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
                count += point.count
            } else if (statistic == 'count') {
                value += point.count
                count += point.count
            } else if (statistic.match(/^p[0-9]+/)) {
                pvalues = pvalues.concat(point.pvalues || [])
            } /* avg */ else {
                value += point.sum
                count += point.count
            }
        }
        if (statistic.match(/^p[0-9]+/)) {
            let p = parseInt(statistic.slice(1))
            pvalues.sort((a, b) => a - b)
            let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
            value = pvalues[nth]
        } else if (statistic == 'avg') {
            value /= count || 1
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{value, timestamp: this.timestamp * 1000, count}],
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
        let i = 0
        for (let point of span.points) {
            let value: number | undefined
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
                    let pvalues = point.pvalues || []
                    pvalues.sort((a, b) => a - b)
                    let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1)
                    value = pvalues[nth]
                } /* avg */ else {
                    value = point.sum / (point.count || 1)
                }
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
        }
    }

    /*
        Convert a dimensions object of the form {key: value, ...} to a string with comma separated "key=value,..."
     */
    private makeDimensionString(dimensions: MetricDimensions): string {
        let result: string[] = []
        for (let [name, value] of Object.entries(dimensions || {})) {
            if (Array.isArray(value)) {
                /* istanbul ignore next */
                this.log.error(`Dimension is an array`, {value, dimensions})
            }
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
        Add a point value to the metric at a desired timestamp and/or span index
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
        let points = span.points
        let start = span.end - points.length * interval

        //  Stop recursing once the desired span period is reached
        let period = span.period < queryPeriod ? queryPeriod : 0

        if (points.length) {
            /* istanbul ignore next */
            if (timestamp < start) {
                this.log.error('Bad span', {metric, point, timestamp, start, span})
                return
            }
            let shift = 0
            if (queryPeriod) {
                if (period) {
                    //  Propagate all points if not on the target period
                    shift = points.length
                }
            } else if (point.count) {
                //  Adding one more to make room for this point being added
                shift = (timestamp - start) / interval - span.samples + 1
            }
            shift = Math.max(0, Math.min(shift, points.length))
            this.assert(0 <= shift && shift <= points.length)

            while (shift-- > 0) {
                let p = points.shift()
                //  Recurse and add the point to the next metric
                if (p.count && si + 1 < metric.spans.length) {
                    this.addValue(metric, start, p, si + 1, period)
                }
                start += interval
            }
        }
        if (period) {
            if (si + 1 < metric.spans.length) {
                this.addValue(metric, timestamp, point, si + 1, period)
            }
        } else if (point.count) {
            if (points.length == 0) {
                start = span.end = this.getTimestamp(span, timestamp)
            }
            while (timestamp < start) {
                points.unshift({count: 0, sum: 0})
                start -= interval
            }
            while (timestamp >= span.end) {
                points.push({count: 0, sum: 0})
                span.end += interval
            }
            this.assert(points.length <= span.samples)

            let index = Math.floor((timestamp - start) / interval)
            this.assert(0 <= index && index < points.length)
            this.setPoint(span, index, point)
            // span.points[index].when = timestamp
        }
    }

    /*
        Add value to a span and update count, min, max, pValues and sum
        The point "index" defines the point[] to update.
     */
    private setPoint(span: Span, index: number, add: Point) {
        let points = span.points
        this.assert(0 <= index && index < points.length)
        let point = points.at(index)!
        if (!point) {
            /* istanbul ignore next */
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
                point.pvalues.push(...(add.pvalues || [add.sum / add.count]))
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
        */
        if (ttl) {
            //  OneTable converts to DynamoDB seconds
            metric.expires = new Date((this.timestamp + ttl) * 1000)
        }
        let where: string | undefined = undefined
        if (metric.seq != undefined) {
            let seq = (metric.seq = metric.seq || 0)
            if (metric.seq++ >= MaxSeq) {
                /* istanbul ignore next */
                metric.seq = 0
            }
            where = `\${seq} = {${seq}}`
        }
        /*
            Create should be cheaper than update. Disable OneTable timestamps for the metric items.
            Use a sequence number to detect simultaneous updated. If collision, will throw 
            a ConditionalCheckFailedException and emit() will then retry.
        */
        let stats = {}
        let result = await this.db.create('Metric', metric, {
            exists: null,
            timestamps: false,
            return: false,
            stats,
            where,
        })
        let size = JSON.stringify(result).length
        this.log.info(
            `Emit metric: ${metric.namespace}/${metric.metric}/${metric.dimensions} = ${point.sum}, msize ${size}`,
            {
                metric,
                point,
                stats,
            }
        )
    }

    /*
        Get list of metrics at a given level. The args: namespace and metrics may be undefined.
        Return {namespaces, metrics, dimensions} as possible.
        DOC: add text about pagination and size limits
     */
    async getMetricList(
        namespace: string = undefined,
        metric: string = undefined,
        options: MetricListOptions = {fields: ['pk', 'sk'], limit: MetricListLimit}
    ): Promise<MetricList> {
        if (!options.fields) {
            options.fields = ['pk', 'sk']
        }
        let map = {} as any
        let next: object | undefined
        let owner = options.owner || this.owner
        do {
            options.next = next
            options.log = this.options.log == 'verbose' ? true : false
            let list = await this.db.find('Metric', {owner, namespace, version: Version}, options)
            if (list.length) {
                for (let item of list) {
                    let ns = (map[item.namespace] = map[item.namespace] || {})
                    let met = (ns[item.metric] = ns[item.metric] || [])
                    met.push(item.dimensions)
                }
            }
            next = list.next
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
}

export {Schema, Version}
