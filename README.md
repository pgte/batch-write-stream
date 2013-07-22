# batch-write-stream

[![Build Status](https://travis-ci.org/pgte/batch-write-stream.png?branch=master)](https://travis-ci.org/pgte/batch-write-stream)

## Install

```bash
$ npm install batch-write-stream --save
```

## Use

### Require

```javascript
var BatchWriteStream = require('batch-write-stream');
```

### Create

```javascript
var stream = BatchWriteStream();
```

Or, with options:

```javascript
var options = {
  highWaterMark: 100,  // default
  maxConcurrentBatches: 1 // default
};

var stream = BatchWriteStream(options);
```

### Implement _writeBatch

```javascript
stream._writeBatch = function(batch, cb) {
  // batch is an array
  // call cb when done
}
```

## License

MIT