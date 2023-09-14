import process from 'process';
import { DynamoDBClient, GetItemCommand, PutItemCommand, QueryCommand, } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
const Version = 1;
const Assert = true;
const Buffering = true;
const DefaultResolution = 0;
const MaxSeq = Number.MAX_SAFE_INTEGER;
const MaxRetries = 10;
const MetricListLimit = 10000;
export const DefaultSpans = [
    { period: 5 * 60, samples: 10 },
    { period: 60 * 60, samples: 12 },
    { period: 24 * 60 * 60, samples: 12 },
    { period: 7 * 24 * 60 * 60, samples: 14 },
    { period: 28 * 24 * 60 * 60, samples: 14 },
    { period: 365 * 24 * 60 * 60, samples: 12 },
];
var Instances = {};
process.on('SIGTERM', async () => {
    await CustomMetrics.terminate();
});
export class CustomMetrics {
    consistent = false;
    buffer;
    buffers = {};
    client;
    log;
    options;
    owner;
    prefix = 'metric';
    primaryKey;
    sortKey;
    pResolution;
    source;
    spans;
    table;
    timestamp;
    type;
    ttl;
    constructor(options = {}) {
        this.log = new Log(options.log);
        if (options.ttl && typeof options.ttl != 'number') {
            throw new Error('Bad type for "ttl" option');
        }
        if (options.spans && (!Array.isArray(options.spans) || options.spans.length == 0)) {
            throw new Error('The "spans" option must be an non-empty array');
        }
        if (options.source && typeof options.source != 'string') {
            throw new Error('Non-string "source" option');
        }
        if (options.pResolution != undefined && (options.pResolution < 0 || options.pResolution > 1000)) {
            throw new Error('Invalid "pResolution" option. Must be between 0 and 1000. Default is 0');
        }
        if (options.consistent != null && typeof options.consistent != 'boolean') {
            throw new Error('Bad type for "consistent" option');
        }
        if (options.prefix) {
            this.prefix = options.prefix;
        }
        if (options.buffer) {
            if (typeof options.buffer != 'object') {
                throw new Error('Bad type for "buffer" option');
            }
            this.buffer = options.buffer;
        }
        this.primaryKey = options.primaryKey || 'pk';
        this.sortKey = options.sortKey || 'sk';
        this.type = options.type || { _type: 'Metric' };
        if (options.client) {
            this.client = options.client;
        }
        else {
            let params = {};
            if (options.creds) {
                params.credentials = options.creds;
                params.region = params.credentials.region;
            }
            if (options.region) {
                params.region = options.region;
            }
            this.client = new DynamoDBClient(params);
        }
        if (!options.table && !options.tableName) {
            throw new Error('Missing DynamoDB table name property');
        }
        this.table = options.table || options.tableName;
        this.options = options;
        this.owner = options.owner || 'default';
        this.spans = options.spans || DefaultSpans;
        this.ttl = options.ttl || this.spans[this.spans.length - 1].period;
        if (options.consistent != null) {
            this.consistent = options.consistent;
        }
        if (options.source) {
            this.source = options.source;
        }
        this.pResolution = options.pResolution || DefaultResolution;
    }
    async emit(namespace, metricName, value, dimensionsList = [{}], options = {}) {
        if (value == undefined || value == null) {
            throw new Error('Invalid metric value');
        }
        value = Number(value);
        if (isNaN(value)) {
            throw new Error(`Value to emit is not valid`);
        }
        if (!namespace || !metricName) {
            throw new Error('Missing emit namespace / metric argument');
        }
        if (!Array.isArray(dimensionsList)) {
            throw new Error('Dimensions must be an array');
        }
        if (dimensionsList.length == 0) {
            dimensionsList = [{}];
        }
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        let point;
        point = { count: 1, sum: value };
        return await this.emitDimensions(namespace, metricName, point, dimensionsList, options);
    }
    async emitDimensions(namespace, metricName, point, dimensionsList, options) {
        let result;
        for (let dim of dimensionsList) {
            let dimensions = this.makeDimensionString(dim);
            let buffer = options.buffer || this.buffer;
            if (buffer && Buffering) {
                result = await this.bufferMetric(namespace, metricName, point, dimensions, options);
            }
            else {
                result = await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options);
            }
        }
        return result;
    }
    async emitDimensionedMetric(namespace, metricName, point, dimensions, options = {}) {
        let ttl = options.ttl != undefined ? options.ttl : this.ttl;
        let retries = MaxRetries;
        let metric;
        let backoff = 10;
        let chan = options.log == true ? 'info' : 'trace';
        do {
            let owner = options.owner || this.owner;
            metric = await this.getMetric(owner, namespace, metricName, dimensions, options.log);
            if (!metric) {
                metric = this.initMetric(owner, namespace, metricName, dimensions);
            }
            if (point.timestamp) {
                let si = metric.spans.findIndex((s) => s.end - s.period <= point.timestamp && point.timestamp < s.end);
                if (si >= 0) {
                    this.addValue(metric, point.timestamp, point, si);
                }
                else {
                    this.log.error('Cannot determine span index', { point, metric });
                }
            }
            else {
                this.addValue(metric, point.timestamp, point, 0);
            }
            if (this.source) {
                metric._source = this.source;
            }
            if (ttl) {
                metric.expires = this.timestamp + ttl;
            }
            if (await this.putMetric(metric, options)) {
                break;
            }
            if (retries == 0) {
                this.log.error(`Metric update has too many retries`, { namespace, metricName, dimensions });
                break;
            }
            this.log[chan](`Retry ${MaxRetries - retries} metric update ${metric.namespace} ${metric.metric} ${metric.dimensions}`, {
                retries,
                metric,
            });
            backoff = backoff * 2;
            this.log[chan](`Retry backoff ${backoff} ${this.jitter(backoff)}`);
            await this.delay(this.jitter(backoff));
        } while (retries-- > 0);
        return metric;
    }
    async bufferMetric(namespace, metricName, point, dimensions, options) {
        let buffer = options.buffer || this.buffer;
        let interval = this.spans[0].period / this.spans[0].samples;
        let key = `${namespace}|${metricName}|${JSON.stringify(dimensions)}`;
        let elt = (this.buffers[key] = this.buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: this.timestamp + (buffer.elapsed || interval),
            namespace: namespace,
            metric: metricName,
            dimensions,
        });
        if (buffer.force ||
            (buffer.sum && elt.sum >= buffer.sum) ||
            (buffer.count && elt.count >= buffer.count) ||
            this.timestamp >= elt.timestamp) {
            point.timestamp = elt.timestamp;
            await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options);
        }
        elt.count += point.count;
        elt.sum += point.sum;
        CustomMetrics.saveInstance({ key }, this);
        return {
            spans: [{ points: [{ count: elt.count, sum: elt.sum }] }],
            metric: metricName,
            namespace: namespace,
            owner: options.owner || this.owner,
            version: Version,
        };
    }
    static async terminate() {
        await CustomMetrics.flushAll();
    }
    static async flushAll() {
        for (let [key, instance] of Object.entries(Instances)) {
            await instance.flush();
            CustomMetrics.freeInstanceByKey(key);
        }
        Instances = {};
    }
    async flush() {
        for (let elt of Object.values(this.buffers)) {
            let point = { count: elt.count, sum: elt.sum };
            await this.emitDimensionedMetric(elt.namespace, elt.metric, point, elt.dimensions);
        }
        this.buffers = {};
    }
    async query(namespace, metricName, dimensions, period, statistic, options = {}) {
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        let owner = options.owner || this.owner;
        await this.flush();
        let dimString = this.makeDimensionString(dimensions);
        let metric = await this.getMetric(owner, namespace, metricName, dimString, options.log);
        if (!metric) {
            return { dimensions, id: options.id, metric: metricName, namespace, period, points: [], owner, samples: 0 };
        }
        let span = metric.spans.find((s) => period <= s.period);
        if (!span) {
            span = metric.spans[metric.spans.length - 1];
            period = span.period;
        }
        this.addValue(metric, this.timestamp, { count: 0, sum: 0 }, 0, period);
        let result;
        if (metric && span) {
            if (options.accumulate) {
                result = this.accumulateMetric(metric, span, statistic, owner);
            }
            else {
                result = this.calculateSeries(metric, span, statistic, owner);
            }
        }
        else {
            result = { dimensions, metric: metricName, namespace, period, points: [], owner, samples: span.samples };
        }
        result.id = options.id;
        this.log[options.log == true ? 'info' : 'trace'](`Query metrics ${namespace}, ${metricName}`, {
            dimensions,
            period,
            statistic,
            options,
            result,
        });
        return result;
    }
    accumulateMetric(metric, span, statistic, owner) {
        let value = 0, count = 0, pvalues = [];
        if (statistic == 'max') {
            value = Number.NEGATIVE_INFINITY;
        }
        else if (statistic == 'min') {
            value = Infinity;
        }
        else if (statistic == 'sum') {
            value = 0;
            count = 0;
        }
        else if (statistic == 'count') {
            value = 0;
            count = 0;
        }
        else if (statistic.match(/^p[0-9]+/)) {
            pvalues = [];
        }
        else {
            value = 0;
            count = 0;
        }
        let points = span.points;
        for (let i = 0; i < points.length; i++) {
            let point = points[i];
            if (statistic == 'max') {
                if (point.max != undefined) {
                    value = Math.max(value, point.max);
                }
            }
            else if (statistic == 'min') {
                if (point.min != undefined) {
                    value = Math.min(value, point.min);
                }
            }
            else if (statistic == 'sum') {
                value += point.sum;
            }
            else if (statistic == 'count') {
                value += point.count;
            }
            else if (statistic.match(/^p[0-9]+/)) {
                pvalues = pvalues.concat(point.pvalues);
            }
            else {
                value += point.sum;
            }
            count += point.count;
        }
        if (statistic.match(/^p[0-9]+/)) {
            let p = parseInt(statistic.slice(1));
            pvalues.sort((a, b) => a - b);
            let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1);
            value = pvalues[nth];
        }
        else if (statistic == 'avg') {
            value /= Math.max(count, 1);
        }
        let timestamp = (this.timestamp || Date.now()) * 1000;
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{ value, timestamp, count }],
            samples: span.samples,
        };
    }
    calculateSeries(metric, span, statistic, owner) {
        let points = [];
        let interval = span.period / span.samples;
        let timestamp = span.end - span.points.length * interval;
        let value = undefined;
        let i = 0;
        for (let point of span.points) {
            if (point.count > 0) {
                if (statistic == 'max') {
                    if (point.max != undefined) {
                        if (value == undefined) {
                            value = point.max;
                        }
                        else {
                            value = Math.max(value, point.max);
                        }
                    }
                }
                else if (statistic == 'min') {
                    if (point.min != undefined) {
                        if (value == undefined) {
                            value = point.min;
                        }
                        else {
                            value = Math.min(value, point.min);
                        }
                    }
                }
                else if (statistic == 'sum') {
                    value = point.sum;
                }
                else if (statistic == 'count') {
                    value = point.count;
                }
                else if (statistic.match(/^p[0-9]+/)) {
                    let p = parseInt(statistic.slice(1));
                    let pvalues = point.pvalues;
                    pvalues.sort((a, b) => a - b);
                    let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1);
                    value = pvalues[nth];
                }
                else {
                    value = point.sum / point.count;
                }
            }
            else {
                value = 0;
            }
            timestamp += interval;
            timestamp = Math.min(timestamp, this.timestamp);
            points.push({ value, count: point.count, timestamp: timestamp * 1000 });
            i++;
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            period: span.period,
            points: points,
            owner: owner,
            samples: span.samples,
        };
    }
    makeDimensionString(dimensions) {
        let result = [];
        let entries = Object.entries(dimensions).sort((a, b) => a[0].localeCompare(b[0]));
        for (let [name, value] of entries) {
            result.push(`${name}=${value}`);
        }
        return result.join(',');
    }
    makeDimensionObject(dimensions) {
        let result = {};
        for (let dimension of dimensions.split(',')) {
            if (dimension) {
                let [key, value] = dimension.split('=');
                result[key] = value;
            }
        }
        return result;
    }
    addValue(metric, timestamp = this.timestamp, point, si, queryPeriod = 0) {
        this.assert(metric);
        this.assert(timestamp);
        this.assert(0 <= si && si < metric.spans.length);
        let span = metric.spans[si];
        let interval = span.period / span.samples;
        let points = span.points || [];
        let start = span.end - points.length * interval;
        let aggregate = !queryPeriod || span.period < queryPeriod ? true : false;
        while (points.length > span.samples) {
            points.shift();
        }
        if (points.length) {
            if (timestamp < start) {
                this.log.error('Bad metric span', { metric, point, timestamp, start, span });
                return;
            }
            let shift = 0;
            if (queryPeriod && aggregate) {
                shift = points.length;
            }
            else if (point.count) {
                shift = Math.floor((timestamp - start) / interval) - span.samples + 1;
            }
            else if (queryPeriod) {
                shift = Math.floor((timestamp - start) / interval) - span.samples;
            }
            shift = Math.max(0, Math.min(shift, points.length));
            this.assert(0 <= shift && shift <= points.length);
            while (shift-- > 0) {
                let p = points.shift();
                if (aggregate && p.count && si + 1 < metric.spans.length) {
                    this.addValue(metric, start, p, si + 1, queryPeriod);
                }
                start += interval;
            }
        }
        if (aggregate && queryPeriod && si + 1 < metric.spans.length) {
            this.addValue(metric, timestamp, point, si + 1, queryPeriod);
        }
        else if (point.count) {
            if (points.length == 0) {
                start = span.end = this.getTimestamp(span, timestamp);
            }
            while (timestamp < start) {
                points.unshift({ count: 0, sum: 0 });
                start -= interval;
            }
            while (timestamp >= span.end) {
                points.push({ count: 0, sum: 0 });
                span.end += interval;
            }
            this.assert(points.length <= span.samples);
            let index = Math.floor((timestamp - start) / interval);
            this.assert(0 <= index && index < points.length);
            this.setPoint(span, index, point);
        }
    }
    setPoint(span, index, add) {
        let points = span.points;
        this.assert(0 <= index && index < points.length);
        let point = points[index];
        if (!point) {
            this.log.error(`Metric null point`, { span, index, add });
            return;
        }
        if (add.count) {
            let value = add.sum / add.count;
            if (point.min == undefined) {
                point.min = value;
            }
            else {
                point.min = Math.min(value, point.min);
            }
            if (point.max == undefined) {
                point.max = value;
            }
            else {
                point.max = Math.max(value, point.max);
            }
        }
        if (this.pResolution) {
            point.pvalues = point.pvalues || [];
            if (add.pvalues) {
                point.pvalues.push(...add.pvalues);
            }
            else {
                point.pvalues.push(add.sum / add.count);
            }
            point.pvalues.splice(0, point.pvalues.length - this.pResolution);
        }
        point.sum += add.sum;
        point.count += add.count;
    }
    async getMetricList(namespace = undefined, metric = undefined, options = { limit: MetricListLimit }) {
        let map = {};
        let owner = options.owner || this.owner;
        let next;
        let limit = options.limit || MetricListLimit;
        let chan = options.log == true ? 'info' : 'trace';
        let items, command;
        do {
            ;
            ({ command, items, next } = await this.findMetrics(owner, namespace, metric, limit, next));
            this.log[chan](`Find metrics ${namespace}, ${metric}`, { command, items });
            if (items.length) {
                for (let item of items) {
                    let ns = (map[item.namespace] = map[item.namespace] || {});
                    let met = (ns[item.metric] = ns[item.metric] || []);
                    met.push(item.dimensions);
                }
            }
        } while (next);
        let result = { namespaces: Object.keys(map) };
        if (namespace && map[namespace]) {
            result.metrics = Object.keys(map[namespace]);
            if (metric) {
                let dimensions = map[namespace][metric];
                if (dimensions) {
                    result.dimensions = [];
                    dimensions = dimensions.sort().filter((v, index, self) => self.indexOf(v) === index);
                    for (let dimension of dimensions) {
                        result.dimensions.push(this.makeDimensionObject(dimension));
                    }
                }
            }
        }
        return result;
    }
    initMetric(owner, namespace, name, dimensions) {
        let metric = {
            dimensions,
            metric: name,
            namespace,
            owner,
            spans: [],
            version: Version,
        };
        for (let sdef of this.spans) {
            let span = {
                samples: sdef.samples,
                period: sdef.period,
                end: this.timestamp,
                points: [],
            };
            span.end = this.getTimestamp(span);
            metric.spans.push(span);
        }
        return metric;
    }
    async getMetric(owner, namespace, metric, dimensions, log) {
        let command = new GetItemCommand({
            TableName: this.table,
            Key: {
                [this.primaryKey]: { S: `${this.prefix}#${Version}#${owner}` },
                [this.sortKey]: { S: `${this.prefix}#${namespace}#${metric}#${dimensions}` },
            },
            ConsistentRead: this.consistent,
        });
        let data = await this.client.send(command);
        let result = null;
        if (data && data.Item) {
            let item = unmarshall(data.Item);
            result = this.mapItemFromDB(item);
        }
        if (log == true) {
            let chan = log == true ? 'info' : 'trace';
            this.log[chan](`GetMetric ${namespace}, ${metric} ${dimensions}`, { cmd: command, result });
        }
        return result;
    }
    async findMetrics(owner, namespace, metric, limit, startKey) {
        let key = [namespace];
        if (metric) {
            key.push(metric);
        }
        let start = startKey ? marshall(startKey) : undefined;
        let command = new QueryCommand({
            TableName: this.table,
            ExpressionAttributeNames: {
                '#_0': this.primaryKey,
                '#_1': this.sortKey,
            },
            ExpressionAttributeValues: {
                ':_0': { S: `${this.prefix}#${Version}#${owner}` },
                ':_1': { S: `${this.prefix}#${key.join('#')}` },
            },
            KeyConditionExpression: '#_0 = :_0 and begins_with(#_1, :_1)',
            ConsistentRead: this.consistent,
            Limit: limit,
            ScanIndexForward: true,
            ExclusiveStartKey: start,
            ProjectionExpression: `${this.primaryKey}, ${this.sortKey}`,
        });
        let result = await this.client.send(command);
        let items = [];
        if (result.Items) {
            for (let i = 0; i < result.Items.length; i++) {
                let item = unmarshall(result.Items[i]);
                items.push(this.mapItemFromDB(item));
            }
        }
        let next = undefined;
        if (result.LastEvaluatedKey) {
            next = unmarshall(result.LastEvaluatedKey);
        }
        return { items, next, command };
    }
    async putMetric(item, options) {
        let ConditionExpression, ExpressionAttributeValues;
        let seq;
        if (item.seq != undefined) {
            seq = item.seq = item.seq || 0;
            if (item.seq++ >= MaxSeq) {
                item.seq = 0;
            }
            ConditionExpression = `seq = :_0`;
            ExpressionAttributeValues = { ':_0': { N: seq.toString() } };
        }
        else {
            item.seq = 0;
        }
        let mapped = this.mapItemToDB(item);
        let params = {
            TableName: this.table,
            ReturnValues: 'NONE',
            Item: marshall(mapped, { removeUndefinedValues: true }),
            ConditionExpression,
            ExpressionAttributeValues,
        };
        let command = new PutItemCommand(params);
        let chan = options.log == true ? 'info' : 'trace';
        this.log[chan](`Put metric ${item.namespace}, ${item.metric}`, { dimensions: item.dimensions, command, params, item });
        try {
            await this.client.send(command);
            return true;
        }
        catch (err) {
            ;
            (function (err, log) {
                let code = err.code || err.name;
                if (code == 'ConditionalCheckFailedException') {
                    log.trace(`Update collision`, { err });
                }
                else if (code == 'ProvisionedThroughputExceededException') {
                    log.info(`Provisioned throughput exceeded: ${err.message}`, { err, cmd: command, item });
                }
                else {
                    log.error(`Emit exception code ${err.name} ${err.code} message ${err.message}`, {
                        err,
                        cmd: command,
                        item,
                    });
                    throw err;
                }
                return false;
            })(err, this.log);
        }
    }
    mapItemFromDB(data) {
        let pk = data[this.primaryKey];
        let sk = data[this.sortKey];
        let owner = pk.split('#').pop();
        let [, namespace, metric, dimensions] = sk.split('#');
        let spans;
        if (data.spans) {
            spans = data.spans.map((s) => {
                return {
                    end: s.se,
                    period: s.sp,
                    samples: s.ss,
                    points: s.pt.map((p) => {
                        let point = { count: Number(p.c), sum: Number(p.s) };
                        if (p.x != null) {
                            point.max = Number(p.x);
                        }
                        if (p.m != null) {
                            point.min = Number(p.m);
                        }
                        if (p.v) {
                            point.pvalues = p.v;
                        }
                        return point;
                    }),
                };
            });
        }
        let expires = data.expires;
        let seq = data.seq;
        return { dimensions, expires, metric, namespace, owner, seq, spans };
    }
    mapItemToDB(item) {
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
                        let p = { c: point.count, s: this.round(point.sum) };
                        if (point.max != null) {
                            p.x = this.round(point.max);
                        }
                        if (point.min != null) {
                            p.m = this.round(point.min);
                        }
                        if (point.pvalues) {
                            p.v = point.pvalues;
                        }
                        return p;
                    }),
                };
            }),
            seq: item.seq,
            _source: item._source,
        };
        if (this.type) {
            let [key, model] = Object.entries(this.type)[0];
            result[key] = model;
        }
        return result;
    }
    static allocInstance(tags, options = {}) {
        let key = JSON.stringify(tags);
        let metrics = Instances[key];
        if (!metrics) {
            metrics = Instances[key] = new CustomMetrics(options);
        }
        return metrics;
    }
    static freeInstance(tags) {
        let key = JSON.stringify(tags);
        delete Instances[key];
    }
    static freeInstanceByKey(key) {
        delete Instances[key];
    }
    static getInstance(tags) {
        let key = JSON.stringify(tags);
        return Instances[key];
    }
    static saveInstance(tags, metrics) {
        let key = JSON.stringify(tags);
        Instances[key] = metrics;
    }
    static getCache() {
        return Instances;
    }
    getTimestamp(span, timestamp = this.timestamp) {
        let interval = span.period / span.samples;
        return Math.ceil(timestamp / interval) * interval;
    }
    assert(c) {
        if (!c && Assert) {
            let msg = { stack: '' };
            if (typeof Error.captureStackTrace === 'function') {
                Error.captureStackTrace(msg);
            }
            else {
                msg.stack = new Error('Assert').stack;
            }
            this.log.error(`Assertion failed ${msg.stack}`);
        }
    }
    info(message, context = {}) {
        console.log('INFO: ' + message, context);
    }
    error(message, context = {}) {
        console.log('ERROR: ' + message, context);
    }
    trace(message, context = {}) {
        console.log('TRACE: ' + message, context);
    }
    round(n) {
        if (isNaN(n) || n == null) {
            return 0;
        }
        let places = 16 - n.toFixed(0).length;
        return Number(n.toFixed(places)) - 0;
    }
    jitter(msecs) {
        return Math.min(10 * 1000, Math.floor(msecs / 2 + msecs * Math.random()));
    }
    async delay(time) {
        return new Promise(function (resolve, reject) {
            setTimeout(() => resolve(true), time);
        });
    }
}
class Log {
    senselogs = null;
    logger = null;
    verbose = false;
    constructor(dest) {
        if (dest === true) {
            this.logger = this.defaultLogger;
        }
        else if (dest == 'verbose') {
            this.logger = this.defaultLogger;
            this.verbose = true;
        }
        else if (dest && typeof dest.info == 'function') {
            this.senselogs = dest;
        }
    }
    error(message, context) {
        this.process('error', message, context);
    }
    info(message, context) {
        this.process('info', message, context);
    }
    trace(message, context) {
        this.process('trace', message, context);
    }
    process(chan, message, context) {
        if (this.logger) {
            this.logger(chan, message, context);
        }
        else if (this.senselogs) {
            this.senselogs[chan](message, context);
        }
    }
    defaultLogger(chan, message, context) {
        if (chan == 'trace' && !this.verbose) {
            return;
        }
        let tag = chan.toUpperCase();
        if (context) {
            try {
                console.log(tag, message, JSON.stringify(context, null, 4));
            }
            catch (err) {
                let buf = ['{'];
                for (let [key, value] of Object.entries(context)) {
                    try {
                        buf.push(`    ${key}: ${JSON.stringify(value, null, 4)}`);
                    }
                    catch (err) {
                    }
                }
                buf.push('}');
                console.log(tag, message, buf.join('\n'));
            }
        }
        else {
            console.log(tag, message);
        }
    }
}
