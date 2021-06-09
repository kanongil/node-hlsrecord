'use strict';

const { once } = require('events');
const Fs = require('fs');
const Http = require('http');
const Https = require('https');
const Path = require('path');
const Process = require('process');
const { Stream } = require('stream');
const Url = require('url');
const { promisify } = require('util');
const debug = require('debug')('hls:uploader');

const AgentKeepalive = require('agentkeepalive');
const Got = require('got');
const Hoek = require('@hapi/hoek');
const Minio = require('minio');
const Pati = require('pati');
const Uuid = require('uuid');
const WriteFileAtomic = require('write-file-atomic');


const internals = {
    pipeline: promisify(Stream.pipeline),

    async toBuffer(stream) {

        const chunks = [];
        let length = 0;
        for await (const chunk of stream) {
            chunks.push(chunk);
            length += chunk.length;
        }

        return Buffer.concat(chunks, length);
    }
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
    async flushIndex(m3u8, { cacheDuration } = {}) {

        const { ended, target_duration } = m3u8;
        const index = m3u8.toString().trim();

        try {
            await Promise.all([...this.pushing]);
        }
        catch {}

        return this.impl.flushIndex(index, { cacheDuration, ended, target_duration });
    }

    /* async */ prepare(exclusive, test) {

        return this.impl.prepare(exclusive, test);
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

    async flushIndex(indexString) {

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

    async prepare(exclusive, test) {

        if (!exclusive) {
            if (test) {
                try {
                    await this.got.get('.');
                }
                catch (err) {
                    if (!(err instanceof Got.HTTPError) || err.response.statusCode !== 404) {
                        throw err;
                    }
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

    async flushIndex(indexString) {

        const response = await this.got.put(this.indexName, {
            body: indexString,
            headers: {
                'content-type': 'application/vnd.apple.mpegURL'
            }
        });

        return response.url;
    }
}


internals.createTransport = function (useSSL = false) {

    const http = useSSL ? Https : Http;
    const agentClass = useSSL ? AgentKeepalive.HttpsAgent : AgentKeepalive;

    const agent = new agentClass({
        maxSockets: 6,
        maxFreeSockets: 6,
        timeout: 10000,
        freeSocketTimeout: 30000
    });

    return {
        request(options, next) {

            options.agent = agent;

            const req = http.request(options, next);

            const timeout = setTimeout(() => {

                if (!req.destroyed) {
                    req.destroy(new Error('timeout'));
                }
            }, 30000);

            req.on('close', () => {

                clearTimeout(timeout);
            });

            return req;
        }
    };
};

internals.transport = {
    http: internals.createTransport(false),
    https: internals.createTransport(true)
};


class S3UploaderImpl {

    static async credentials() {

        if (!S3UploaderImpl._credentials) {
            const { defaultProvider } = await import('@aws-sdk/credential-provider-node');
            S3UploaderImpl._credentials = await defaultProvider()();
        }

        return S3UploaderImpl._credentials;
    }

    /**
     * @param {URL} url
     * @param {*} options
     */
    constructor(url, options) {

        this.indexName = options.indexName;
        this.defaultCacheDuration = +options.cacheDuration || 7 * 24 * 3600 * 1000;

        this.Bucket = url.hostname;
        this.ACL = 'public-read';

        this.baseKey = (url.pathname || '/').slice(1);

        /** @type { Minio.ClientOptions } */
        this.config = {
            endPoint: 's3.amazonaws.com',
            region: url.searchParams.get('region'),
            useSSL: true
        };

        const endpoint = url.searchParams.get('endpoint');
        if (endpoint) {
            const endpointUrl = new URL(endpoint);
            endpointUrl.pathname = `/${this.Bucket}/`;

            this.config.endPoint = endpointUrl.host;
            this.config.useSSL = (endpointUrl.protocol === 'https:');
        }

        // Use custom transport with keep-alive and timeout

        this.config.transport = internals.transport[this.config.useSSL ? 'https' : 'http'];

        /** @type { Minio.Client } */
        this.s3 = undefined;
    }

    async prepare(exclusive, test) {

        const creds = await S3UploaderImpl.credentials();

        this.config.accessKey = creds.accessKeyId;
        this.config.secretKey = creds.secretAccessKey;
        this.config.sessionToken = creds.sessionToken;

        this.s3 = new Minio.Client(this.config);

        if (!exclusive) {
            if (test) {
                await this.putObject(this.baseKey, Buffer.alloc(0), null, 0);
            }

            return;
        }

        const uuid = Uuid.v4();

        for (let i = 2; i >= 0; --i) {
            try {
                const failReason = this.obtainLock(uuid);
                if (!failReason) {
                    return;
                }

                i = 0;
                throw new Error(failReason);
            }
            catch (err) {
                if (i === 0) {
                    throw err;
                }
            }

            await Hoek.wait(500);
        }
    }

    async getLockState(uuid) {

        // States: 'locked', 'open', 'conflict:<id>'
        // throws on endpoint errors

        try {
            const stream = await this.s3.getObject(this.Bucket, this.baseKey);
            const body = await internals.toBuffer(stream);

            if (body.toString() !== uuid) {
                return `conflict:${body.toString()}`;
            }
        }
        catch (err) {
            if (err.code === 'NotFound') {
                return 'open';
            }

            throw err;
        }

        return 'locked';
    }

    async obtainLock(uuid) {

        // Note: This lock could fail if it takes longer for a check + write than the wait period

        // Check

        const checkState = await this.getLockState(uuid);
        if (checkState !== 'open') {
            return checkState === 'locked' ? false : `lock object exists: ${checkState}`;
        }

        // Lock probably does not exist - Create with UUID

        try {
            await this.putObject(this.baseKey, Buffer.from(uuid), null, 0);
        }
        catch (err) { }

        // Wait - allow any other writers to finish writing

        Hoek.wait(2500);

        // Verify - check that we won the race

        const verifyState = await this.getLockState(uuid);
        if (verifyState !== 'locked') {
            if (verifyState === 'open') {
                throw new Error('lock is still open');
            }

            return `lock validation failed: ${verifyState} != ${uuid}`;
        }

        return false;
    }

    async pushSegment(stream, name, meta) {

        const key = Path.join(this.baseKey, name);

        /** @type { Minio.ItemBucketMetadata } */
        const headers = {
            'x-amz-acl': this.ACL,
            'Content-Type': meta.mime || 'video/MP2T',
            'Cache-Control': `public, max-age=300, s-maxage=${Math.floor(this.defaultCacheDuration / 1000)}, immutable`
        };

        let bytesWritten = 0;
        // eslint-disable-next-line no-return-assign
        stream.on('data', (chunk) => bytesWritten += +chunk.length );

        try {
            const buffer = await internals.toBuffer(stream);

            Hoek.assert(!(meta.size >= 0) || buffer.length === meta.size, 'Size must match');

            await this.putObject(key, buffer, headers);
            debug('upload finished for', key);

            // TODO: validate returned etag

            return { url: `s3://${this.Bucket}/${key}`, bytesWritten };
        }
        catch (err) {
            debug('upload failed for', key, err);
            throw err;
        }
    }

    async flushIndex(indexString, { cacheDuration, ended, target_duration }) {

        const cacheTime = ended !== false ? (cacheDuration || this.defaultCacheDuration) : target_duration * 1000 / 2;
        const key = Path.join(this.baseKey, this.indexName);

        const headers = {
            'x-amz-acl': this.ACL,
            'Content-Type': 'application/vnd.apple.mpegURL',
            'Cache-Control': `max-age=${Math.floor(cacheTime / 1000)}, public`
        };

        await this.putObject(key, indexString, headers);

        return `s3://${this.Bucket}/${key}`;
    }

    async putObject(key, buffer, headers, retries = 5) {

        try {
            await this.s3.putObject(this.Bucket, key, buffer, headers);
        }
        catch (err) {
            if (retries <= 0) {
                throw err;
            }

            await Hoek.wait(10 ** (5 - retries)); // Up to 10^5 = 100s

            return this.putObject(key, buffer, headers, retries - 1);
        }
    }
}


module.exports = HlsUploader;
