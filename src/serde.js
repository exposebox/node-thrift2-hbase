const Int64 = require('node-int64');

function serialize(valueObj) {
    let buffer;

    if (typeof valueObj != 'object') {
        return valueObj.toString();
    }

    switch (valueObj.type) {
        case 'string':
            return valueObj.value.toString();
        case 'json':
            return JSON.stringify(valueObj.value);
        case 'integer':
        case 'integer32':
            buffer = new Buffer(4);
            buffer.writeInt32BE(valueObj.value, 0);
            return buffer;
        case 'float':
            buffer = new Buffer(4);
            buffer.writeFloatBE(valueObj.value, 0);
            return buffer;
        case 'double':
            buffer = new Buffer(8);
            buffer.writeDoubleBE(valueObj.value, 0);
            return buffer;
        case 'number':
        case 'integer48':
            buffer = new Buffer(8);
            buffer.writeIntBE(valueObj.value, 2, 6);
            return buffer;
        case 'UInteger48':
            buffer = new Buffer(6);
            buffer.writeUIntBE(valueObj.value, 0);
            return buffer;
        case 'long':
        case 'int64':
            return valueObj.value.buffer;
        default:
            return valueObj.toString();
    }
}

function deserialize(buffer, type) {
    switch (type) {
        case 'string':
            return buffer.toString();
        case 'json':
            return JSON.parse(buffer.toString());
        case 'integer':
        case 'integer32':
            return buffer.readInt32BE();
        case 'float':
            return buffer.readFloatBE();
        case 'double':
            return buffer.readDoubleBE();
        case 'number':
        case 'integer48':
            return buffer.readIntBE(2, 6);
        case 'UInteger48':
            return buffer.readUIntBE(0);
        case 'long':
        case 'int64':
            return new Int64(buffer);
        default:
            return buffer.toString();
    }
}

module.exports = {
    serialize,
    deserialize
};