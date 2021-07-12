'use strict';

exports.streamToBuffer = async function (stream) {

    const chunks = [];
    let length = 0;
    for await (const chunk of stream) {
        chunks.push(chunk);
        length += chunk.length;
    }

    return Buffer.concat(chunks, length);
};
