const Hypercore = require('hypercore');

// Static test key pair obtained with:
//
//   const crypto = require('hypercore-crypto');
//   const keyPair = crypto.keyPair();
//   console.log("public key", keyPair.publicKey.toString('hex').match(/../g).join(' '));
//   console.log("secret key", keyPair.secretKey.toString('hex').match(/../g).join(' '));
const testKeyPair = {
    publicKey: Buffer.from([
        0x97, 0x60, 0x6c, 0xaa, 0xd2, 0xb0, 0x8c, 0x1d, 0x5f, 0xe1, 0x64, 0x2e, 0xee, 0xa5, 0x62, 0xcb,
        0x91, 0xd6, 0x55, 0xe2, 0x00, 0xc8, 0xd4, 0x3a, 0x32, 0x09, 0x1d, 0x06, 0x4a, 0x33, 0x1e, 0xe3]),
    secretKey: Buffer.from([
        0x27, 0xe6, 0x74, 0x25, 0xc1, 0xff, 0xd1, 0xd9, 0xee, 0x62, 0x5c, 0x96, 0x2b, 0x57, 0x13, 0xc3,
        0x51, 0x0b, 0x71, 0x14, 0x15, 0xf3, 0x31, 0xf6, 0xfa, 0x9e, 0xf2, 0xbf, 0x23, 0x5f, 0x2f, 0xfe,
        0x97, 0x60, 0x6c, 0xaa, 0xd2, 0xb0, 0x8c, 0x1d, 0x5f, 0xe1, 0x64, 0x2e, 0xee, 0xa5, 0x62, 0xcb,
        0x91, 0xd6, 0x55, 0xe2, 0x00, 0xc8, 0xd4, 0x3a, 0x32, 0x09, 0x1d, 0x06, 0x4a, 0x33, 0x1e, 0xe3]),
}

if (process.argv.length !== 4) {
    console.error("Usage: node interop.js [test step] [test set]")
    process.exit(1);
}

if (process.argv[2] === '1') {
    step1Create(process.argv[3]).then(result => {
        console.log("step1 ready", result);
    });
} else if (process.argv[2] === '2'){
    step2AppendHelloWorld(process.argv[3]).then(result => {
        console.log("step2 ready", result);
    });
} else if (process.argv[2] === '3'){
    step3ReadAndAppendUnflushed(process.argv[3]).then(result => {
        console.log("step3 ready", result);
    });
} else if (process.argv[2] === '4'){
    step4AppendWithFlush(process.argv[3]).then(result => {
        console.log("step4 ready", result);
    });
} else if (process.argv[2] === '5'){
    step5ClearSome(process.argv[3]).then(result => {
        console.log("step5 ready", result);
    });
} else {
    console.error(`Invalid test step {}`, process.argv[2]);
    process.exit(2);
}

async function step1Create(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    await core.close();
};

async function step2AppendHelloWorld(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    const result = await core.append([Buffer.from('Hello'), Buffer.from('World')]);
    assert(result.length, 2);
    assert(result.byteLength, 10);
    await core.close();
};

async function step3ReadAndAppendUnflushed(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    const hello = (await core.get(0)).toString();
    const world = (await core.get(1)).toString();
    assert(hello, "Hello");
    assert(world, "World");
    let result = await core.append(Buffer.from('first'));
    assert(result.length, 3);
    assert(result.byteLength, 15);
    result = await core.append([Buffer.from('second'), Buffer.from('third')]);
    assert(result.length, 5);
    assert(result.byteLength, 26);
    const multiBlock = Buffer.alloc(4096*3, 'a');
    result = await core.append(multiBlock);
    assert(result.length, 6);
    assert(result.byteLength, 12314);
    result = await core.append([]);
    assert(result.length, 6);
    assert(result.byteLength, 12314);
    const first = (await core.get(2)).toString();
    assert(first, "first");
    const second = (await core.get(3)).toString();
    assert(second, "second");
    const third = (await core.get(4)).toString();
    assert(third, "third");
    const multiBlockRead = await core.get(5);
    if (!multiBlockRead.equals(multiBlock)) {
        throw new Error(`Read buffers don't equal, ${multiBlockRead} but expected ${multiBlock}`);
    }
    await core.close();
};

async function step4AppendWithFlush(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    for (let i=0; i<5; i++) {
        result = await core.append(Buffer.from([i]));
        assert(result.length, 6+i+1);
        assert(result.byteLength, 12314+i+1);
    }
}

async function step5ClearSome(testSet) {
    const core = new Hypercore(`work/${testSet}`, testKeyPair.publicKey, {keyPair: testKeyPair});
    await core.clear(5);
    await core.clear(7, 9);
    let info = await core.info();
    assert(info.length, 11);
    assert(info.byteLength, 12319);
    assert(info.contiguousLength, 5);
    assert(info.padding, 0);

    let missing = await core.get(5, { wait: false });
    assert(missing, null);
    missing = await core.get(7, { wait: false });
    assert(missing, null);
    missing = await core.get(8, { wait: false });
    assert(missing, null);
    const third = (await core.get(4)).toString();
    assert(third, "third");
}

function assert(real, expected) {
    if (real != expected)  {
        throw new Error(`Got ${real} but expected ${expected}`);
    }
}
