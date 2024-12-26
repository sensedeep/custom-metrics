"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CustomMetrics = exports.DefaultSpans = void 0;
const process_1 = __importDefault(require("process"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const Version = 1;
const Assert = true;
const Buffering = true;
const DefaultResolution = 0;
const MaxSeq = Number.MAX_SAFE_INTEGER;
const MaxRetries = 10;
const MetricListLimit = 10000;
exports.DefaultSpans = [
    { period: 5 * 60, samples: 10 },
    { period: 60 * 60, samples: 12 },
    { period: 24 * 60 * 60, samples: 12 },
    { period: 7 * 24 * 60 * 60, samples: 14 },
    { period: 28 * 24 * 60 * 60, samples: 14 },
    { period: 365 * 24 * 60 * 60, samples: 12 },
];
var Instances = {};
process_1.default.on('SIGTERM', async () => {
    await CustomMetrics.terminate();
});
class CustomMetrics {
    constructor(options = {}) {
        this.consistent = false;
        this.buffers = null;
        this.prefix = 'metric';
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
        this.expires = options.expires || 'expires';
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
            this.client = new client_dynamodb_1.DynamoDBClient(params);
        }
        if (!options.table) {
            throw new Error('Missing DynamoDB table name property');
        }
        this.table = options.table;
        this.options = options;
        this.owner = options.owner || 'default';
        this.spans = options.spans || exports.DefaultSpans;
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
        if (dimensionsList.length == 0) {
            dimensionsList = [{}];
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
        let point;
        point = { count: 1, sum: value };
        return await this.emitDimensions(namespace, metricName, point, dimensionsList, options);
    }
    async emitDimensions(namespace, metricName, point, dimensionsList, options) {
        let result;
        for (let dim of dimensionsList) {
            let dimensions = this.makeDimensionString(dim);
            let buffer = options.buffer || this.buffer;
            if (buffer && (buffer.elapsed || buffer.force || buffer.sum || buffer.count) && Buffering) {
                result = await this.bufferMetric(namespace, metricName, point, dimensions, options);
            }
            else {
                result = await this.emitDimensionedMetric(namespace, metricName, point, dimensions, options);
            }
        }
        return result;
    }
    async bufferMetric(namespace, metricName, point, dimensions, options) {
        let buffer = options.buffer || this.buffer;
        let key = this.getBufferKey(namespace, metricName, dimensions);
        let buffers = (this.buffers = this.buffers || {});
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        let elapsed = buffer.elapsed || this.spans[0].period / this.spans[0].samples;
        let elt = (buffers[key] = buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: timestamp + elapsed,
            elapsed: elapsed,
            namespace: namespace,
            metric: metricName,
            dimensions,
            spans: [{ points: [{ count: 0, sum: 0 }] }],
        });
        let current = elt.spans[0].points.at(-1);
        if (current) {
            current.count += point.count;
            current.sum += point.sum;
        }
        elt.count += point.count;
        elt.sum += point.sum;
        if (buffer.force ||
            (buffer.sum && elt.sum >= buffer.sum) ||
            (buffer.count && elt.count >= buffer.count) ||
            timestamp >= elt.timestamp) {
            options = Object.assign({}, options, { timestamp: timestamp * 1000 });
            let metric = await this.emitDimensionedMetric(namespace, metricName, elt, dimensions, options);
            elt.count = elt.sum = 0;
            elt.spans = metric.spans;
            elt.timestamp = timestamp + (buffer.elapsed || this.spans[0].period / this.spans[0].samples);
            return metric;
        }
        CustomMetrics.saveInstance({ key }, this);
        return {
            spans: elt.spans,
            metric: metricName,
            namespace: namespace,
            owner: options.owner || this.owner,
            version: Version,
        };
    }
    async emitDimensionedMetric(namespace, metricName, point, dimensions, options = {}) {
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        let ttl = options.ttl != undefined ? options.ttl : this.ttl;
        let retries = MaxRetries;
        let metric;
        let backoff = 10;
        let chan = options.log == true ? 'info' : 'trace';
        do {
            let owner = options.owner || this.owner;
            metric = await this.getMetric(owner, namespace, metricName, dimensions, options.log);
            if (metric) {
                if (options.upgrade) {
                    metric = this.upgradeMetric(metric);
                }
            }
            else {
                metric = this.initMetric(owner, namespace, metricName, dimensions, timestamp);
            }
            if (point.timestamp) {
                let si = metric.spans.findIndex((s) => s.end - s.period <= point.timestamp || s.end <= point.timestamp);
                if (si >= 0) {
                    this.addValue(metric, point.timestamp, point, si);
                }
                else {
                }
            }
            else {
                this.addValue(metric, timestamp, point, 0);
            }
            if (this.source) {
                metric._source = this.source;
            }
            if (ttl) {
                metric.expires = timestamp + ttl;
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
    async upgrade(namespace, metricName, dimensionsList = [{}], options = {}) {
        let owner = options.owner || this.owner;
        if (dimensionsList.length == 0) {
            dimensionsList = [{}];
        }
        let metric;
        for (let dim of dimensionsList) {
            let dimensions = this.makeDimensionString(dim);
            let old = await this.getMetric(owner, namespace, metricName, dimensions, options.log);
            metric = this.upgradeMetric(old);
            await this.putMetric(metric, options);
        }
        return metric;
    }
    upgradeMetric(old) {
        let required = false;
        if (this.spans.length == old.spans.length) {
            for (let [index, span] of Object.entries(old.spans)) {
                if (span.period != this.spans[index].period || span.samples != this.spans[index].samples) {
                    required = true;
                }
            }
            if (!required) {
                return old;
            }
        }
        let timestamp = Math.min(...old.spans.map((span) => span.end - span.period)) || Math.floor(Date.now() / 1000);
        let metric = this.initMetric(old.owner, old.namespace, old.metric, old.dimensions, timestamp);
        for (let span of old.spans) {
            let interval = span.period / span.samples;
            let timestamp = span.end - span.points.length * interval;
            let si = metric.spans.findIndex((s) => s.end - s.period <= timestamp || s.end <= timestamp);
            for (let point of span.points) {
                this.addValue(metric, timestamp, point, si);
                timestamp += interval;
            }
        }
        return metric;
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
        if (!this.buffers)
            return;
        let now = Date.now() / 1000;
        for (let elt of Object.values(this.buffers)) {
            await this.flushElt(elt, now);
        }
    }
    async flushElt(elt, timestamp) {
        elt.timestamp = Math.min(timestamp, elt.timestamp);
        let metric = await this.emitDimensionedMetric(elt.namespace, elt.metric, elt, elt.dimensions, {
            timestamp: elt.timestamp * 1000,
        });
        elt.count = elt.sum = 0;
        elt.spans = metric.spans;
        elt.timestamp = timestamp + (elt.elapsed || this.spans[0].period / this.spans[0].samples);
    }
    getBufferKey(namespace, metricName, dimensions) {
        return `${namespace}|${metricName}|${JSON.stringify(dimensions)}`;
    }
    async query(namespace, metricName, dimensions, period, statistic, options = {}) {
        let owner = options.owner || this.owner;
        let dimString = this.makeDimensionString(dimensions);
        if (period > this.spans.at(-1).period) {
            period = this.spans.at(-1).period;
        }
        let timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        if (this.buffers) {
            let key = this.getBufferKey(namespace, metricName, dimString);
            if (this.buffers[key]) {
                await this.flushElt(this.buffers[key], timestamp);
            }
        }
        let metric = await this.getMetric(owner, namespace, metricName, dimString, options.log);
        if (!metric) {
            return { dimensions, id: options.id, metric: metricName, namespace, period, points: [], owner, samples: 0 };
        }
        let start;
        let si;
        if (options.start) {
            start = options.start / 1000;
            si = metric.spans.findIndex((s) => period <= s.period && s.end - s.period <= start && start <= s.end);
        }
        else {
            let span = metric.spans[0];
            let interval = span.period / span.samples;
            let t = this.roundTime(span, timestamp + 1);
            if (span.end - interval <= t && t <= span.end) {
                start = t - period;
            }
            else {
                start = timestamp - period;
            }
            si = metric.spans.findIndex((s) => period <= s.period);
        }
        if (si < 0) {
            si = metric.spans.length - 1;
        }
        let span = metric.spans[si];
        start = this.roundTime(span, start);
        this.addValue(metric, timestamp, { count: 0, sum: 0 }, 0, si);
        let result;
        if (options.accumulate) {
            result = this.accumulateMetric(metric, span, statistic, owner, start, period);
        }
        else {
            result = this.calculateSeries(metric, span, statistic, owner, start, period);
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
    accumulateMetric(metric, span, statistic, owner, start, period) {
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
        else if (statistic == 'current') {
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
        let interval = span.period / span.samples;
        let t = span.end - span.points.length * interval;
        for (let i = 0; i < points.length; i++) {
            let point = points[i];
            if (start <= t && t < start + period) {
                if (statistic == 'max') {
                    if (point.max != undefined) {
                        value = Math.max(value, point.max);
                    }
                    else {
                        value = Math.max(value, point.sum / (point.count || 1));
                    }
                }
                else if (statistic == 'min') {
                    if (point.min != undefined) {
                        value = Math.min(value, point.min);
                    }
                    else {
                        value = Math.min(value, point.sum / (point.count || 1));
                    }
                }
                else if (statistic == 'sum') {
                    value += point.sum;
                }
                else if (statistic == 'current') {
                    value = point.sum / (point.count || 1);
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
            t += interval;
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
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{ value, timestamp: start + period, count }],
            samples: span.samples,
        };
    }
    calculateSeries(metric, span, statistic, owner, start, period) {
        let points = [];
        let interval = span.period / span.samples;
        let t;
        let firstPoint = span.end - span.points.length * interval;
        let count = Math.floor((firstPoint - start) / interval);
        for (t = start; t < firstPoint && points.length < span.samples; t += interval) {
            points.push({ value: 0, count: 0, timestamp: t * 1000 });
        }
        t = firstPoint;
        for (let point of span.points) {
            if (start <= t && t < start + period) {
                let value = undefined;
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
                points.push({ value, count: point.count, timestamp: (t + interval) * 1000 });
            }
            t += interval;
        }
        count = Math.ceil(period / interval);
        while (points.length < count) {
            points.push({ value: 0, count: 0, timestamp: t * 1000 });
            t += interval;
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
    addValue(metric, timestamp, point, si, querySpanIndex = undefined) {
        this.assert(metric);
        this.assert(timestamp);
        this.assert(0 <= si && si < metric.spans.length);
        let span = metric.spans[si];
        let interval = span.period / span.samples;
        let points = span.points || [];
        let queryRecurse = si < querySpanIndex && si + 1 < metric.spans.length;
        while (points.length > span.samples) {
            points.shift();
        }
        let first = span.end - points.length * interval;
        let shift = 0;
        if (points.length) {
            if (queryRecurse) {
                shift = points.length;
            }
            else if (timestamp >= first) {
                shift = Math.floor((timestamp - first) / interval) - span.samples;
                if (!queryRecurse && point.count && timestamp >= span.end) {
                    shift += 1;
                }
            }
            shift = Math.max(0, Math.min(shift, points.length));
            this.assert(0 <= shift && shift <= points.length);
            for (let i = 0; i < shift; i++) {
                let p = points.shift();
                if (p.count && si + 1 < metric.spans.length) {
                    this.addValue(metric, first, p, si + 1, querySpanIndex);
                }
                first += interval;
            }
        }
        if (queryRecurse) {
            this.addValue(metric, timestamp, point, si + 1, querySpanIndex);
            return;
        }
        if (point.count) {
            let index;
            if (points.length == 0) {
                points.push({ count: 0, sum: 0 });
                span.end = this.roundTime(span, timestamp + 1);
                first = span.end - interval;
                index = 0;
            }
            else {
                if (timestamp < span.end - span.period) {
                    return;
                }
                while (timestamp < first) {
                    points.unshift({ count: 0, sum: 0 });
                    first -= interval;
                }
                while (timestamp >= span.end) {
                    points.push({ count: 0, sum: 0 });
                    span.end += interval;
                }
                index = Math.floor((timestamp - first) / interval);
            }
            this.assert(points.length <= span.samples);
            if (!(0 <= index && index < points.length)) {
                this.assert(0 <= index && index < points.length);
                if (index > 0) {
                    index = points.length - 1;
                }
            }
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
        let next = options.next;
        let limit = options.limit || MetricListLimit;
        let chan = options.log == true ? 'info' : 'trace';
        let items, command;
        let count = 0;
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
                count += items.length;
            }
        } while (next && count < limit);
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
    initMetric(owner, namespace, name, dimensions, timestamp) {
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
                end: timestamp,
                points: [],
            };
            span.end = this.roundTime(span, timestamp + 1);
            metric.spans.push(span);
        }
        return metric;
    }
    async getMetric(owner, namespace, metric, dimensions, log) {
        let command = new client_dynamodb_1.GetItemCommand({
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
            let item = (0, util_dynamodb_1.unmarshall)(data.Item);
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
        let start = startKey ? (0, util_dynamodb_1.marshall)(startKey) : undefined;
        let command = new client_dynamodb_1.QueryCommand({
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
                let item = (0, util_dynamodb_1.unmarshall)(result.Items[i]);
                items.push(this.mapItemFromDB(item));
            }
        }
        let next = undefined;
        if (result.LastEvaluatedKey) {
            next = (0, util_dynamodb_1.unmarshall)(result.LastEvaluatedKey);
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
            Item: (0, util_dynamodb_1.marshall)(mapped, { removeUndefinedValues: true }),
            ConditionExpression,
            ExpressionAttributeValues,
        };
        let command = new client_dynamodb_1.PutItemCommand(params);
        let chan = options.log == true ? 'info' : 'trace';
        this.log[chan](`Put metric ${item.namespace}, ${item.metric}`, {
            dimensions: item.dimensions,
            command,
            params,
            item,
        });
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
        let expires = data[this.expires];
        let seq = data.seq;
        return { dimensions, expires, metric, namespace, owner, seq, spans };
    }
    mapItemToDB(item) {
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
    static freeInstanceByKey(key) {
        delete Instances[key];
    }
    static saveInstance(tags, metrics) {
        let key = JSON.stringify(tags);
        Instances[key] = metrics;
    }
    roundTime(span, timestamp) {
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
            this.log.error(`Assertion failed`, { stack: msg.stack });
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
exports.CustomMetrics = CustomMetrics;
class Log {
    constructor(dest) {
        this.senselogs = null;
        this.logger = null;
        this.verbose = false;
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
