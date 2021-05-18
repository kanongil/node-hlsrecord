'use strict';

const { ok: assert } = require('assert');
const Path = require('path');
const Url = require('url');

const Bounce = require('@hapi/bounce');
const Mime = require('mime-types');
const StreamEach = require('stream-each');
const M3U8Parse = require('m3u8parse');
const debug = require('debug')('hls:recorder');

const HlsUploader = require('./hls-uploader');
const SegmentDecrypt = require('./segment-decrypt');

// eslint-disable-next-line no-unused-vars
const { HlsSegmentReader, HlsSegmentStreamer, HlsStreamerObject, HlsReaderObject } = require('hls-segment-reader');

const { AttrList, MediaSegment } = M3U8Parse;

// TODO: IFRAMES

// Add custom extensions

Mime.extensions['audio/aac'] = ['aac'];
Mime.extensions['audio/ac3'] = ['ac3'];
Mime.extensions['video/iso.segment'] = ['m4s'];


const pathOrUriToURL = function (dirpathOrUri) {

    try {
        var url = new URL(dirpathOrUri);
    }
    catch (err) {
        url = Url.pathToFileURL(dirpathOrUri);
    }

    if (!url.pathname.endsWith('/')) {
        url.pathname = url.pathname + '/';
    }

    return url;
};


const urlWithSearch = function (url, base) {

    url = new URL(url, base);
    url.search = base.search;

    return url;
};


const HlsStreamRecorder = class HlsStreamRecorder {

    /**
     * @param {HlsSegmentReader} reader
     * @param {string} dst
     * @param {*} options
     */
    constructor(reader, dst, options) {

        options = options || {};

        assert(reader instanceof HlsSegmentReader);

        this.reader = reader;
        this.dst = pathOrUriToURL(dst); // target directory / s3 url

        this.nextSegmentMsn = -1;
        this.msn = 0;
        this.index = null;

        this.startOffset = parseFloat(options.startOffset);
        this.subreader = options.subreader;
        this.collect = !!options.collect; // collect into a single file (v4 feature)
        this.decrypt = options.decrypt;
        this.exclusive = options.exclusive;

        this.recorders = [];

        this.mapMsn = 0;
        this.nextMap = null;
        this.segmentExt = null;

        this.streamer = null;
        this.uploader = new HlsUploader(this.dst, { collect: this.collect });
        this.segmentHead = 0;

        this.maxSegmentBw = 0;
        this.maxSegmentBwAvg = 0;
        this.segmentBandwidths = [];

        this.ended = {};
        this.ended.promise = new Promise((resolve, reject) => {

            this.ended.resolve = resolve;
            this.ended.reject = reject;
        });

        this.reader.on('error', this.ended.reject);
        this.reader.on('close', this.onReaderClose.bind(this));
    }

    async onReaderClose() {

        try {
            if (!this.index) {
                throw new Error('premature close');
            }

            if (!this.index.master) {
                this.index.ended = true;
            }

            await Promise.race([
                this.ended.promise,
                (async () => {

                    await this.flushIndex();
                    await Promise.all(this.recorders.map((r) => r.ended.promise));

                    if (this.index.master) {
                        this.setBandwidthFromRecorders();

                        await this.flushIndex();
                    }
                })()
            ]);
        }
        catch (err) {
            return this.ended.reject(err);
        }

        this.ended.resolve();
        debug('done');
    }

    start() {

        assert(!this.reader.index, 'Reader already active');

        this.reader.on('index', this.updateIndex.bind(this));
        this.reader.pause();

        this.uploader.prepare(this.exclusive).catch((err) => {

            this.ended.reject(err);
            this.reader.destroy();
        });

        return this;
    }

    stop(immediate) {

        const abort = immediate ? new Error('aborted') : null;
        try {
            this.reader.destroy(abort);
            this.recorders.forEach((r) => r.reader.destroy(abort));
        }
        catch (err) {
            if (err !== abort) {
                throw err;
            }
        }
    }

    async completed() {

        let thrownErr;
        try {
            try {
                await this.ended.promise;
            }
            catch (err) {
                thrownErr = err;
            }

            this.stop(!!thrownErr);

            await Promise.all(this.recorders.map((r) => {

                return r.completed().catch((err) => {

                    // Abort graceful stop() on any errors

                    if (!thrownErr) {
                        thrownErr = err;
                        this.stop(true);
                    }
                });
            }));
        }
        finally {
            if (thrownErr) {
                // eslint-disable-next-line no-unsafe-finally
                throw thrownErr;
            }
        }
    }

    /**
     * @param {M3U8Parse.MasterPlaylist} master
     * @param {import('hls-segment-reader/lib').HlsIndexMeta} meta
     */
    setupMaster(master, meta) {

        this.index = master = new M3U8Parse.MasterPlaylist(master);

        debug('variants', master.variants);
        if (this.subreader) {

            // Remove backup sources

            const used = new Set();
            master.variants = master.variants.filter((variant) => {

                if (variant.info) {
                    const bw = variant.info.get('bandwidth', AttrList.Types.Int);
                    const res = !used.has(bw);
                    used.add(bw);
                    return res;
                }

                return true;
            });

            const Recorder = /** @type {typeof HlsStreamRecorder} */(this.constructor); // use subclassed HlsStreamRecorder class

            master.variants.forEach((variant, index) => {

                const variantUrl = Url.resolve(meta.url, variant.uri);
                debug('url', variantUrl);

                // check for duplicate source urls
                let rec = this.recorderForUrl(variantUrl);
                if (!rec || !rec.localUrl) {
                    const dir = this.variantName(variant.info, index);
                    rec = new Recorder(this.subreader(variantUrl), urlWithSearch(dir, this.dst).href, { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
                    rec.localUrl = Url.format({ pathname: Path.join(dir, 'index.m3u8') });
                    rec.remoteUrl = variantUrl;

                    this.recorders.push(rec);
                }

                variant.uri = rec.localUrl;
            });

            const allGroups = [];
            for (const groupItems of master.groups.values()) {
                Array.prototype.push.apply(allGroups, groupItems);
            }

            allGroups.forEach((groupItem, index) => {

                const srcUri = groupItem.get('uri', AttrList.Types.String);
                if (srcUri) {
                    const itemUrl = Url.resolve(meta.url, srcUri);
                    debug('url', itemUrl);

                    let rec = this.recorderForUrl(itemUrl);
                    if (!rec || !rec.localUrl) {
                        const dir = this.groupSrcName(groupItem, index);
                        rec = new Recorder(this.subreader(itemUrl), urlWithSearch(dir, this.dst).href, { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
                        rec.localUrl = Url.format({ pathname: Path.join(dir, 'index.m3u8') });
                        rec.remoteUrl = itemUrl;

                        this.recorders.push(rec);
                    }

                    groupItem.set('uri', rec.localUrl, AttrList.Types.String);
                }
            });

            // Start all recordings

            const inheritError = (recorder, err) => {

                if (err.message !== 'aborted') {
                    console.error(`[${recorder.localUrl.slice(0, -11)}] recorder crash`, err);
                }

                this.ended.reject(err);
            };

            for (const recording of this.recorders) {
                recording.completed().catch(inheritError.bind(this, recording));
                recording.start();
            }

            master.iframes = [];
        }
        else {
            master.variants = [];
            master.groups = new Map();
            master.iframes = [];
        }

        // Trigger 'end' event

        this.reader.resume();
    }

    /**
     * @param {M3U8Parse.MediaPlaylist} media
     */
    setupMedia(media) {

        const orig = media;
        this.index = media = new M3U8Parse.MediaPlaylist(media);

        if (this.collect) {
            media.version = Math.max(4, media.version);
        }  // v4 is required for byterange support

        media.version = Math.max(2, media.version);    // v2 is required to support the remapped IV attribute
        if (media.version !== orig.version) {
            debug('changed index version to:', media.version);
        }

        media.segments = [];
        media.media_sequence = this.msn;
        media.type = 'EVENT';
        media.ended = false;
        media.discontinuity_sequence = 0; // not allowed in event playlists

        if (!isNaN(this.startOffset)) {
            let offset = this.startOffset;
            if (!orig.ended) {
                if (offset < 0) {
                    offset = Math.min(offset, -3 * media.target_duration);
                }
            }

            media.start = new AttrList();
            media.start.set('time-offset', offset, AttrList.Types.SignedFloat);
        }

        // Strip delivery-specific tags

        delete media.server_control;
        delete media.meta.skip;

        // Strip low-latency tags

        delete media.part_info;
        delete media.meta.preload_hints;
        delete media.meta.rendition_reports;

        // Setup segment streamer

        this.streamer = new HlsSegmentStreamer(this.reader);
        this.streamer.on('error', this.ended.reject);
        this.streamer.on('problem', (err) => {

            console.error('streamer problem', err.stack);
        });

        // TODO: replace with async iterator consume
        StreamEach(this.streamer, this.process.bind(this));
    }

    /**
     * @param {M3U8Parse.MasterPlaylist | M3U8Parse.MediaPlaylist} update
     * @param {import('hls-segment-reader/lib').HlsIndexMeta} meta
     */
    updateIndex(update, meta) {

        if (!this.index) {
            update.master ? this.setupMaster(update, meta) : this.setupMedia(update);

            if (this.decrypt) {
                this.decrypt.base = meta.url;
            }
        }

        // Validate update

        if (this.index.target_duration > update.target_duration) {
            throw new Error('Invalid index');
        }
    }

    setBandwidthFromRecorders() {

        assert(this.index.master);

        for (const variant of this.index.variants) {
            const rec = this.recorderForUrl(variant.uri);
            if (rec && rec.maxSegmentBw) {
                const audioGroups = this.index.groups.get(variant.info.get('audio', AttrList.Types.String));
                let audioBw = 0;
                let audioBwAvg = 0;

                for (const info of audioGroups || []) {
                    const audioRec = this.recorderForUrl(info.get('uri', AttrList.Types.String));
                    audioBw = Math.max(audioBw, audioRec.maxSegmentBw);
                    audioBwAvg = Math.max(audioBwAvg, audioRec.maxSegmentBwAvg);
                }

                variant.info.set('bandwidth', Math.ceil(rec.maxSegmentBw + audioBw), AttrList.Types.Float);
                if (rec.maxSegmentBwAvg) {
                    variant.info.set('average-bandwidth', Math.ceil(rec.maxSegmentBwAvg + audioBwAvg), AttrList.Types.Float);
                }
                else {
                    variant.info.delete('average-bandwidth');
                }
            }
        }
    }

    async process(segmentInfo, done) {

        let result;
        try {
            if (segmentInfo.type === 'segment') {
                return await this.processSegment(segmentInfo);
            }

            if (segmentInfo.type === 'map') {
                return await this.processMap(segmentInfo);
            }

            debug('unknown segment type: ' + segmentInfo.type);
        }
        catch (err) {
            result = err;
        }
        finally {
            done(result);
        }
    }

    /**
     * @param {HlsStreamerObject} segmentInfo
     */
    async processMap(segmentInfo) {

        const meta = segmentInfo.file;
        const uri = `${this.segmentName(this.mapMsn, true)}.${Mime.extension(meta.mime)}`;

        this.mapMsn++;

        try {
            var { bytesWritten } = await this.writeStream(segmentInfo.stream, uri, meta);
        }
        catch (err) {
            Bounce.rethrow(err, 'system');
        }

        const map = new AttrList();

        map.set('uri', uri, AttrList.Types.String);

        // handle byterange

        if (this.collect) {
            map.set('byterange', `${bytesWritten}@${this.uploader.segmentBytes - bytesWritten}`, AttrList.Types.String);
            this.segmentExt = Mime.extension(meta.mime);
        }

        this.nextMap = map;
    }

    /**
     * @param {HlsStreamerObject & { segment: HlsReaderObject }} segmentInfo
     */
    async processSegment(segmentInfo) {

        const segment = new MediaSegment(segmentInfo.segment.entry);
        let meta = segmentInfo.file;

        // mark discontinuities

        if (this.nextSegmentMsn !== -1 &&
        this.nextSegmentMsn !== segmentInfo.segment.msn) {
            segment.discontinuity = true;
        }

        this.nextSegmentMsn = segmentInfo.segment.msn + 1;

        // create our own uri

        segment.uri = `${this.segmentName(this.msn)}.${this.segmentExt || Mime.extension(meta.mime)}`;

        // add map info

        segment.map = null;
        if (this.nextMap) {
            segment.map = this.nextMap;
            this.nextMap = null;
        }

        delete segment.byterange;

        // save the stream segment

        let stream;
        try {
            stream = await hlsrecorder.decrypt(segmentInfo.stream, segmentInfo.segment.entry.keys, this.decrypt);
        }
        catch (err) {
            console.error('decrypt failed', err.stack);
            stream = segmentInfo.stream;
        }

        if (stream !== segmentInfo.stream) {
            segment.keys = undefined;
            meta = { mime: meta.mime, modified: meta.modified }; // size is no longer valid
        }

        this.msn++;

        try {
            var { bytesWritten } = await this.writeStream(stream, segment.uri, meta);
        }
        catch (err) {
            Bounce.rethrow(err, 'system');
        }

        const segmentBw = 8 * bytesWritten / segmentInfo.segment.entry.duration;
        this.maxSegmentBw = Math.max(this.maxSegmentBw, segmentBw);
        this.segmentBandwidths.push(segmentBw);
        if (this.segmentBandwidths.length === 5) {
            const avgBw = this.segmentBandwidths.reduce((total, bw) => total + bw, 0) / 5;
            this.maxSegmentBwAvg = Math.max(this.maxSegmentBwAvg, avgBw);
            this.segmentBandwidths.unshift();
        }

        // Handle byterange

        if (this.collect) {
            const isContigious = this.segmentHead > 0 && ((this.segmentHead + bytesWritten) === this.uploader.segmentBytes);
            segment.byterange = {
                length: bytesWritten,
                offset: isContigious ? undefined : this.uploader.segmentBytes - bytesWritten
            };

            this.segmentHead = this.uploader.segmentBytes;
        }

        // Strip low-latency parts

        delete segment.parts;

        // Update index

        M3U8Parse.MediaPlaylist.cast(this.index).segments.push(segment);

        return this.flushIndex();
    }

    async writeStream(stream, name, meta) {

        try {
            return await this.uploader.pushSegment(stream, name, meta);
        }
        catch (err) {
            console.error(`failed write to ${new URL(name, this.dst)}`, err);
            throw err;
        }
    }

    variantName(info, index) {

        return `v${index}`;
    }

    groupSrcName(info, index) {

        const lang = (info.get('language', AttrList.Types.String) || '').replace(/\W/g, '').toLowerCase();
        const id = (info.get('group-id', AttrList.Types.String) || 'unk').replace(/\W/g, '').toLowerCase();
        return `grp/${id}/${lang ? lang + '-' : ''}${index}`;
    }

    segmentName(msn, isInit) {

        const name = (n) => n;

        return this.collect ? 'stream' : (isInit ? 'init-' : '') + name(msn);
    }

    flushIndex() {

        return this.uploader.flushIndex(this.index);
    }

    recorderForUrl(url) {

        for (const rec of this.recorders) {
            if (rec.remoteUrl === url ||
                rec.localUrl === url) {

                return rec;
            }
        }

        return null;
    }
};


const hlsrecorder = module.exports = function hlsrecorder(reader, dst, options) {

    return new HlsStreamRecorder(reader, dst, options);
};


hlsrecorder.HlsStreamRecorder = HlsStreamRecorder;

hlsrecorder.decrypt = SegmentDecrypt.decrypt;
