'use strict';

const { once } = require('events');
const Fs = require('fs');
const Path = require('path');
const Process = require('process');
const { Stream } = require('stream');
const Url = require('url');
const { promisify } = require('util');
const debug = require('debug')('hls:uploader');

const AgentKeepalive = require('agentkeepalive');
const AwsS3 = require('aws-sdk/clients/s3');
const Got = require('got');
const Hoek = require('@hapi/hoek');
const Pati = require('pati');
const Uuid = require('uuid');
const WriteFileAtomic = require('write-file-atomic');


const internals = {
    pipeline: promisify(Stream.pipeline)
};


class HlsUploader {

    constructor(targetUri, options) {

        const url = new URL(targetUri);

        Hoek.assert(url.pathname.endsWith('/'), 'Url must end with a "/"');
        Hoek.assert(!url.hash, 'Url must not contain a hash part');

        options = Object.assign({}, options, {
            indexName: options.indexName || 'index.m3u8'
        });

        if (url.protocol === 's3:') {
            Hoek.assert(!!options.collect === false, 'Collect not supported with s3:');
            this.impl = new S3UploaderImpl(url, options);
        }
        else if (url.protocol === 'http:' ||
                 url.protocol === 'https:') {

            Hoek.assert(!!options.collect === false, 'Collect not supported with ' + url.protocol);
            this.impl = new HttpUploaderImpl(url, options);
        }
        else if (url.protocol === 'file:') {
            this.impl = new FileUploaderImpl(url, options);
        }

        if (!this.impl) {
            throw new Error('Unsupported protocol: ' + url.protocol);
        }

        this.pushing = new Set();
    }

    /*async*/ pushSegment(stream, name, meta) {

        const promise = this.impl.pushSegment(stream, name, meta);
        this.pushing.add(promise);
        return promise.finally(() => this.pushing.delete(promise));
    }

    /**
     * @param {import('m3u8parse/lib/m3u8parse').M3U8Playlist} m3u8 
     */
    async flushIndex(m3u8) {

        const { ended, target_duration } = m3u8;
        const index = m3u8.toString().trim();

        try {
            await Promise.all([...this.pushing]);
        }
        catch {}

        return this.impl.flushIndex(index, { ended, target_duration });
    }

    /* async */ prepare(exclusive) {

        return this.impl.prepare(exclusive);
    }

    get segmentBytes() {

        return this.impl.segmentBytes;
    }
}


class FileUploaderImpl {

    constructor(url, options) {

        this.collect = !!options.collect;
        this.indexName = options.indexName;
        this.targetPath = url;

        this.lastIndexString = '';
        this.segmentBytes = 0;
    }

    async prepare(exclusive) {

        try {
            await Fs.promises.mkdir(this.targetPath, { recursive: !exclusive });
        }
        catch (err) {
            if (exclusive || err.code !== 'EEXIST') {
                throw err;
            }
        }
    }

    async pushSegment(stream, name, meta) {

        const append = this.collect && this.segmentBytes !== 0;

        const url = new URL(name, this.targetPath);
        const target = Fs.createWriteStream(url, { flags: append ? 'a' : 'w' });
        stream.pipe(target);

        const dispatcher = new Pati.EventDispatcher(stream);

        dispatcher.on('end', Pati.EventDispatcher.end);

        let bytesWritten = 0;
        // eslint-disable-next-line no-return-assign
        dispatcher.on('data', (chunk) => bytesWritten += +chunk.length);

        try {
            // TODO: handle target errors & wait for end?
            await dispatcher.finish();

            Hoek.assert(!(meta.size >= 0) || bytesWritten === meta.size, 'Size must match');

            return { url: url.href, bytesWritten };
        }
        finally {
            this.segmentBytes += bytesWritten;
        }
    }

    async flushIndex(indexString, { ended, target_duration }) {

        let appendString;
        if (this.lastIndexString && indexString.startsWith(this.lastIndexString)) {
            const lastLength = this.lastIndexString.length;
            appendString = indexString.substr(lastLength);
        }

        this.lastIndexString = indexString;

        const url = new URL(this.indexName, this.targetPath);

        if (appendString) {
            await Fs.promises.appendFile(url, appendString);
        }
        else {
            await WriteFileAtomic(Url.fileURLToPath(url), indexString);
        }

        return url.href;
    }
}


class HttpUploaderImpl {

    constructor(url, options) {

        const scheme = url.protocol.slice(0, -1);
        const headers = Object.create(null);

        const authHeader = Process.env.HTTP_AUTH_HEADER;
        if (authHeader) {
            const match = /^\s*(.+?):\s*(.+)\s*$/.exec(authHeader);
            Hoek.assert(match, 'Invalid auth header');
            const [, key, value] = match;
            headers[key] = value;
        }

        this.indexName = options.indexName;

        /** @type { import('got/dist/source').Got } */
        this.got = Got.extend({
            prefixUrl: url,
            headers,
            followRedirect: false,
            http2: false, // Node HTTP2 is broken
            retry: 5,
            timeout: 2 * 60 * 1000,
            agent: {
                [scheme]: new (scheme === 'https' ? AgentKeepalive.HttpsAgent : AgentKeepalive)({
                    maxSockets: 1,
                    maxFreeSockets: 1,
                    timeout: 10000,
                    freeSocketTimeout: 30000
                })
            }
        });
    }

    async prepare(exclusive) {

        if (!exclusive) {
            try {
                await this.got.get('.');
            }
            catch (err) {
                if (!(err instanceof Got.HTTPError) || err.response.statusCode !== 404) {
                    throw err;
                }
            }

            return;
        }

        // Note: This lock could fail if it takes longer for a check + write than the wait period

        const lockName = '.lock';

        // Check that it does not exist

        try {
            await this.got.get(lockName);

            throw new Error('lock object exists');
        }
        catch (err) {
            if (!(err instanceof Got.HTTPError) || err.response.statusCode !== 404) {
                throw err;
            }
        }

        // Lock probably does not exist - Create with UUID

        const uuid = Uuid.v4();

        await this.got.put(lockName, {
            body: uuid
        });

        // Wait

        Hoek.wait(2500);

        // Verify

        const response = await this.got.get(lockName, { responseType: 'text' });

        if (response.body !== uuid) {
            throw new Error(`lock validation failed: ${response.body} != ${uuid}`);
        }
    }

    async pushSegment(stream, name, meta) {

        let bytesWritten = 0;

        // eslint-disable-next-line no-return-assign
        stream.on('data', (chunk) => bytesWritten += +chunk.length);

        try {
            const contentLength = meta.size >= 0 ? meta.size : undefined;
            const got = this.got.stream.put(name, {
                headers: {
                    'content-type': meta.mime || 'video/MP2T',
                    'content-length': contentLength
                }
            });

            const pipeline = internals.pipeline(stream, got);

            got.resume(); // Empty buffer (needed for http keep-alive)

            const [response] = await once(got, 'response');
            await pipeline;

            debug('upload finished for', response.url);

            Hoek.assert(!(contentLength >= 0) || bytesWritten === contentLength, 'Size must match');

            return { url: response.url, bytesWritten };
        }
        catch (err) {
            debug('upload failed for', name, err);
            throw err;
        }
    }

    async flushIndex(indexString, { ended, target_duration }) {

        const response = await this.got.put(this.indexName, {
            body: indexString,
            headers: {
                'content-type': 'application/vnd.apple.mpegURL'
            }
        });

        return response.url;
    }
}


class S3UploaderImpl {

    /**
     * @param {URL} url 
     * @param {*} options 
     */
    constructor(url, options) {

        this.indexName = options.indexName;
        this.cacheDuration = +options.cacheDuration || 7 * 24 * 3600 * 1000;

        this.Bucket = url.host;
        this.ACL = 'public-read';

        /** @type { AWS.S3.ClientConfiguration } */
        const params = {
            maxAttempts: 10,
            region: 'unknown'
        };

        const endpoint = url.searchParams.get('endpoint');
        if (endpoint) {
            const endpointUrl = new URL(endpoint);
            endpointUrl.pathname = `/${this.Bucket}/`;

            params.endpoint = endpointUrl.href;
            params.s3BucketEndpoint = true;
        }

        this.s3 = new AwsS3(params);
        this.baseKey = (url.pathname || '/').slice(1);
    }

    async prepare(exclusive = false) {

        if (!exclusive) {
            try {
                await this.s3.getObject({
                    Bucket: this.Bucket,
                    Key: this.baseKey
                });
            }
            catch (err) {
                if (err.name !== 'NoSuchKey') {
                    throw err;
                }
            }

            return;
        }

        // Note: This lock could fail if it takes longer for a check + write than the wait period

        const lockKey = Path.join(this.baseKey, '.lock');

        // Check

        try {
            await this.s3.getObject({
                Bucket: this.Bucket,
                Key: lockKey
            });

            throw new Error('lock object exists');
        }
        catch (err) {
            if (err.name !== 'NoSuchKey') {
                throw err;
            }
        }

        // Lock probably does not exist - Create with UUID

        const uuid = Uuid.v4();

        await this.s3.putObject({
            Bucket: this.Bucket,
            Key: lockKey,
            ACL: this.ACL,
            Body: uuid
        });

        // Wait

        Hoek.wait(2500);

        // Verify

        const result = await this.s3.getObject({
            Bucket: this.Bucket,
            Key: lockKey
        });

        if (result.Body.toString() !== uuid) {
            throw new Error(`lock validation failed: ${result.Body.toString()} != ${uuid}`);
        }
    }

    async pushSegment(stream, name, meta) {

        /** @type { AWS.S3.PutObjectRequest } */
        const params = {
            Bucket: this.Bucket,
            Key: Path.join(this.baseKey, name),
            ACL: this.ACL,
            Body: stream,
            ContentType: meta.mime || 'video/MP2T',
            CacheControl: `max-age=300, s-max-age=${Math.floor(this.cacheDuration / 1000)}, public`,
            ContentLength: meta.size >= 0 ? meta.size : undefined
        };

        let bytesWritten = 0;
        // eslint-disable-next-line no-return-assign
        stream.on('data', (chunk) => bytesWritten += +chunk.length );

        try {
            await this.s3uploadstream(params);
            debug('upload finished for', params.Key);

            Hoek.assert(!(params.ContentLength >= 0) || bytesWritten === params.ContentLength, 'Size must match');

            return { url: `s3://${params.Bucket}/${params.Key}`, bytesWritten };
        }
        catch (err) {
            debug('upload failed for', params.Key, err);
            throw err;
        }
    }

    async flushIndex(indexString, { ended, target_duration }) {

        const { s3 } = this;

        const cacheTime = ended !== false ? this.cacheDuration : target_duration * 1000 / 2;

        const params = {
            Bucket: this.Bucket,
            Key: Path.join(this.baseKey, this.indexName),
            ACL: this.ACL,
            Body: indexString,
            ContentType: 'application/vnd.apple.mpegURL',
            CacheControl: `max-age=${Math.floor(cacheTime / 1000)}, public`
        };

        await s3.putObject(params);

        return `s3://${params.Bucket}/${params.Key}`;
    }

    async s3uploadstream(params) {

        const { s3 } = this;

        // AWS SDK can seriously mess with streams - just buffer everything

        const chunks = [];
        let length = 0;
        for await (const chunk of params.Body) {
            chunks.push(chunk);
            length += chunk.length;
        }

        params.Body = Buffer.concat(chunks);

        return s3.putObject(params);
    }
}


module.exports = HlsUploader;
