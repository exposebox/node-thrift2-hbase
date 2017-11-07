function serialize(valueObj) {
    if (typeof valueObj != 'object') {
        return valueObj.toString();
    }

    switch (valueObj.type) {
        case "string":
            return valueObj.value.toString();
        case "json":
            return JSON.stringify(valueObj.value);
        case "integer":
        case "integer32":
            var buf = new Buffer(4);
            buf.writeInt32BE(valueObj.value, 0);
            return buf;
        case "float":
            var buf = new Buffer(4);
            buf.writeFloatBE(valueObj.value, 0);
            return buf;
        case "number":
        case "integer48":
            var buf = new Buffer(8);
            buf.writeIntBE(valueObj.value, 2, 6);
            return buf;
        case "UInteger48":
            var buf = new Buffer(6);
            buf.writeUIntBE(valueObj.value, 0);
            return buf;
        case "long":
        case "int64":
            return valueObj.value.toBuffer(true);
        default:
            return valueObj.toString();
    }
}

function deserialize(buf, type) {
    switch (type) {
        case "string":
            return buf.toString();
        case "json":
            return JSON.parse(buf.toString());
        case "integer":
        case "integer32":
            return buf.readInt32BE();
        case "float":
            return buf.readFloatBE();
        case "number":
        case "integer48":
            return buf.readIntBE(2, 6);
        case "UInteger48":
            return buf.readUIntBE(0);
        case "long":
        case "int64":
            return buf.toBuffer(true);
        default:
            return buf.toString();
    }
}

module.exports = {
    serialize,
    deserialize
};