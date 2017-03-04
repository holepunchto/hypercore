var protobuf = require('protocol-buffers')

module.exports = protobuf(`
  message Open {
    required bytes discoveryKey = 1;
    optional bytes nonce = 2;
  }

  message Handshake {
    required bytes id = 1;
  }

  message Active {

  }

  message Inactive {

  }

  message Have {
    required uint64 start = 1;
    optional uint64 length = 2; // default 1
    optional bytes bitfield = 3;
  }

  message Unhave {
    required uint64 start = 1;
    optional uint64 length = 2; // default 1
  }

  message Want {
    required uint64 start = 1;
    optional uint64 length = 2; // default Infinity
  }

  message Unwant {
    required uint64 start = 1;
    optional uint64 length = 2; // default Infinity
  }

  message Request {
    required uint64 index = 1;
    optional uint64 bytes = 2;
    optional bool hash = 3;
    optional uint64 nodes = 4;
  }

  message Cancel {
    required uint64 index = 1;
    optional uint64 bytes = 2;
    optional bool hash = 3;
  }

  message Data {
    message Node {
      required uint64 index = 1;
      required bytes hash = 2;
      required uint64 size = 3;
    }

    required uint64 index = 1;
    optional bytes value = 2;
    repeated Node nodes = 3;
    optional bytes signature = 4;
  }

  message Close {

  }
`)
