'use strict';

const Fs = require('fs');
const Path = require('path');
const Url = require('url');
const debug = require('debug')('hls:uploader');

const Aws = require('aws-sdk');
const Hoek = require('@hapi/hoek');
const Pati = require('pati');
const Uuidv4 = require('uuidv4');
const WriteFileAtomic = require('write-file-atomic');


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
        else if (url.protocol === 'file:') {
            this.impl = new FileUploaderImpl(url, options);
        }
        else {
            Hoek.assert(false, 'Unsupported protocol: ' + url.protocol);
        }
    }

    /*async*/ pushSegment(stream, name, meta) {

        return this.impl.pushSegment(stream, name, meta);
    }

    /*async*/ flushIndex(m3u8) {

        return this.impl.flushIndex(m3u8.toString().trim(), m3u8);
    }

    /* async */ prepare(exclusive) {

        return this.impl.prepare(exclusive);
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
        dispatcher.on('data', (chunk) => {

            bytesWritten += +chunk.length;
        });

        try {
            // TODO: handle target errors & wait for end?
            await dispatcher.finish();

            if (meta.size !== undefined && bytesWritten !== meta.size) {
                throw new Error('FAiled to write all content');
            }

            return { url: url.href, bytesWritten };
        }
        finally {
            this.segmentBytes += bytesWritten;
        }
    }

    async flushIndex(indexString, m3u8) {

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


class S3UploaderImpl {

    constructor(url, options) {

        this.indexName = options.indexName;
        this.cacheDuration = options.cacheDuration || 7 * 24 * 3600 * 1000;

        const params = {
            params: {
                Bucket: url.host,
                ACL: 'public-read'
            }
        };

        const endpoint = url.searchParams.get('endpoint');
        if (endpoint) {
            const endpointUrl = new URL(endpoint);
            endpointUrl.pathname = `/${params.params.Bucket}/`;

            params.endpoint = endpointUrl.href;
            params.s3BucketEndpoint = true;
            delete params.Bucket;
        }

        this.s3 = new Aws.S3(params);
        this.baseKey = (url.pathname || '/').slice(1);
    }

    async prepare(exclusive) {

        if (!exclusive) {
            try {
                await this.s3.getObject({
                    Key: this.baseKey
                }).promise();
            }
            catch (err) {
                if (err.code !== 'NoSuchKey') {
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
                Key: lockKey
            }).promise();

            throw new Error('lock object exists');
        }
        catch (err) {
            if (err.code !== 'NoSuchKey') {
                throw err;
            }
        }

        // Lock probably does not exist - Create with UUID

        const uuid = Uuidv4.uuid();

        await this.s3.putObject({
            Body: uuid,
            Key: lockKey,
            ACL: 'private'
        }).promise();

        // Wait

        Hoek.wait(2500);

        // Verify

        const result = await this.s3.getObject({
            Key: lockKey
        }).promise();

        if (result.Body.toString() !== uuid) {
            throw new Error(`lock validation failed: ${result.Body.toString()} != ${uuid}`);
        }
    }

    async pushSegment(stream, name, meta) {

        Hoek.assert(meta.size || meta.size === 0, 'Missing size');

        const params = {
            Body: stream,
            Key: Path.join(this.baseKey, name),
            ContentType: meta.mime || 'video/MP2T',
            CacheControl: `max-age=300, s-max-age=${Math.floor(this.cacheDuration / 1000)}, public`,
            ContentLength: meta.size
        };

        try {
            const data = await this.s3uploadstream(params);
            debug('upload finished for', params.Key);

            return { url: `s3://${data.Bucket}/${data.Key}`, bytesWritten: meta.size };
        }
        catch (err) {
            debug('upload failed for', params.Key, err);
            throw err;
        }
    }

    async flushIndex(indexString, m3u8) {

        const { s3 } = this;

        const cacheTime = m3u8.ended ? this.cacheDuration : m3u8.target_duration * 1000 / 2;

        const params = {
            Body: indexString,
            Key: Path.join(this.baseKey, this.indexName),
            ContentType: 'application/vnd.apple.mpegURL',
            CacheControl: `max-age=${Math.floor(cacheTime / 1000)}, public`
        };

        const data = await s3.upload(params).promise();

        return `s3://${data.Bucket}/${data.Key}`;
    }

    s3uploadstream(params) {

        const { s3 } = this;

        // Fix broken stream upload for fully buffered readable streams

        const upload = s3.upload(params);
        const promise = upload.promise();
        upload.fillQueue(); // force queue fill
        return promise;
    }
}


module.exports = HlsUploader;
