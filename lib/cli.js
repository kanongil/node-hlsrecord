#!/usr/bin/env node

/* eslint-disable no-process-exit */
'use strict';

// record a live hls-stream storing an on-demand ready version

try {
    var Nopt = require('noptify/node_modules/nopt');
}
catch (e) {
    Nopt = require('nopt');
}

const { HlsSegmentReader } = require('hls-segment-reader');
const Recorder = require('..');

const DateValue = class {};
Nopt.typeDefs[DateValue] = { type: DateValue, validate(data, key, val) {

    let date;
    if (val === 'now') {
        val = '+0';
    }

    if (val.length && (val[0] === '+' || val[0] === '-')) {
        date = new Date(Math.round(new Date().getTime() / 1000 + parseInt(val, 10)) * 1000);
    }
    else if (`${parseInt(val, 10)}` === val) {
        date = new Date(parseInt(val, 10) * 1000);
    }
    else {
        date = new Date(val);
    }

    if (!date) {
        return false;
    }

    data[key] = date;
} };

const HexValue = function () {};
Nopt.typeDefs[HexValue] = { type: HexValue, validate(data, key, val) {

    data[key] = Buffer.from(val, 'hex');
} };

const hlsrecord = require('noptify')(process.argv, { program: 'hlsrecord <url>' });

hlsrecord.version(require('../package').version)
    .option('collect', '-C', 'Collect output segments to a single file', Boolean)
    .option('output', '-o', 'Output directory / s3 bucket url', String)
    .option('create-dir', '-c', 'Explicitly create output dir', Boolean)
    .option('begin-date', '-b', 'Start recording at', DateValue)
    .option('end-date', '-e', 'Stop recording at', DateValue)
    .option('start-offset', '-s', 'Playback start time offset in seconds', Number)
    .option('extension', 'Preserve specified vendor extension', Array)
    .option('low-latency', '-L', 'Fetch as low-latency', Boolean)
    .option('segment-extension', 'Preserve specified vendor segment extension', Array)
    .option('user-agent', '-a', 'HTTP User-Agent', String)
    .option('decrypt', 'Attempt to decrypt segments', Boolean)
    .option('cookie', 'Add cookie header to key requests', String)
    .option('key', 'Use oob hex encoded key to decrypt segments', HexValue)
    .parse(process.argv);

const options = hlsrecord.nopt;
const srcUri = options.argv.remain[0];
if (!srcUri) {
    hlsrecord.help();
    process.exit(-1);
}

const outDir = options.output || 'stream';

if (options['begin-date']) {
    console.log('fetching from:', options['begin-date']);
}

if (options['end-date']) {
    console.log('fetching until:', options['end-date']);
}

const extensions = {};
(options.extension || []).forEach((ext) => {

    extensions[ext] = false;
});
(options['segment-extension'] || []).forEach((ext) => {

    extensions[ext] = true;
});

const readerOptions = {
    fullStream: !options['begin-date'],
    startDate: options['begin-date'],
    stopDate: options['end-date'],
    maxStallTime: 5 * 60 * 1000,
    extensions,
    lowLatency: options['low-latency']
};

const createReader = (uri) => {

    return new HlsSegmentReader(uri, readerOptions);
};

let decrypt = null;
if (options.decrypt) {
    decrypt = {
        cookie: options.cookie,
        key: options.key
    };
}

const rdr = createReader(srcUri);

module.exports = Recorder(rdr, outDir, {
    subreader: createReader,
    startOffset: options['start-offset'],
    collect: !!options.collect,
    exclusive: !!options['create-dir'],
    decrypt
}).start();
