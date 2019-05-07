var fs = require('fs');
var util = require('util');

var RpcClient = require('bitcoind-rpc');
var util = require('util');
const Client = require('bitcoin-core');
const client = new Client({ network: 'regtest', port: 18443, username: '__cookie__', password: '6377ad51031c201505abc5df6ff1df22e5d4a3d29b04ff5d5e7c0f395684b42a' });

const btclib = require('bitcoinjs-lib')

const crypto = require('crypto');

//const bip65 = require('bip65')
//------------------------------------------------------

async function sleep(n) {
    return new Promise(function(resolve, reject){
	setTimeout(resolve, n*1000);
    });
}

//------------------------------------------------------

var cnetwork = btclib.networks.regtest;

class Account {
    
    static fromJson(obj) {
	var self = new Account(obj.pk, obj.sk, obj.addr);
	for(var i in obj.utxos) {
	    self.addUtxo(Utxo.fromJson(obj.utxos[i]));
	}
	return self;
    }
    
    constructor(pk, sk, addr) {
	this.pk = pk;
	this.sk = sk;
	this.addr = addr;
	this.utxos = {};
    }
    
    id() {
	return this.sk;
    }
    
    addUtxo(utxo) {
	this.utxos[utxo.id()] = utxo;
    }
    
    spendUtxo(utxo) {
	var id = utxo.id();
	for(var key in this.utxos) {
	    if(id == key) {
		delete this.utxos[key];
		return;
	    }
	}
    }
    
    spendCoins(coins) {
	for(var i in coins) {
	    delete this.utxos[coins[i].id()];
	}
    }
    
    selectCoins(amount) {
	var coins = [];
	var amt = 0;
	
	for(var key in this.utxos) {
	    var utxo = this.utxos[key];
	    coins.push(utxo);
	    amt += utxo.amount;
	    if(amt >= amount) {
		this.spendCoins(coins);
		return {'utxos': coins, 'amount': amt};
	    }
	}
	return {'utxos': [], 'amount': 0};
    }
    
    print() {
	console.log('account: %s %s %s', this.pk, this.sk, this.addr);
	for(var key in this.utxos) {
	    this.utxos[key].print();
	}
    }
}

class Wallet {
    
    static fromJson(obj) {
	var self = new Wallet();
	for(var i in obj.accounts) {
	    self.addAccount(Account.fromJson(obj.accounts[i]));
	}
	return self;
    }
    
    constructor() {
	this.accounts = {};
    }
    
    addAccount(account) {
	this.accounts[account.id()] = account;
    }
    
    getAccount(pkh) {
	for(var i in this.accounts) {
	    if(this.accounts[i].addr == pkh)
		return i;
	}
	return null;
    }
    
    send2address(from, to, amount) {
	var dst = this.getAccount(to);
	var amt = amount + 50000;
	var ret = this.accounts[from].selectCoins(amt);
	if(ret.amount < amt) {
	    console.log('Insufficient funds');
	    return false;
	}
	var coins = ret.utxos;
	var tx = createTransaction(from, coins, to, amount);
	
	if(tx.outs.length > 1) {
	    this.accounts[from].addUtxo(new Utxo(tx.getId(), 1, from, ret.amount-amt));
	}
	if(dst) {
	    this.accounts[dst].addUtxo(new Utxo(tx.getId(), 0, this.accounts[dst].id(), amount));
	}
	
	console.log('tx:\n', tx.toHex());
	return tx.toHex();
    }
    
    send2addressByHTLC(from, to, amount, x) {
	var amt = amount + 50000;
	var ret = this.accounts[from].selectCoins(amt);
	if(ret.amount < amt) {
	    console.log('Insufficient funds');
	    return false;
	}

	var frompair = sk2ecpair(from);
	
	var hashx = btclib.crypto.sha256(Buffer.from(x, 'hex')).toString('hex');
	var to160 = btclib.crypto.hash160(Buffer.from(to, 'hex'));
	var from160 = btclib.crypto.hash160(frompair.publicKey);

	var now = parseInt(Date.now()/1000);
	var locktime = now + 60;
	var htlc = hashtimelockcontract (hashx, locktime, to160, from160);
	
	var coins = ret.utxos;
	var tx = createTransaction(from, coins, htlc.p2sh, amount);
	
	if(tx.outs.length > 1) {
	    this.accounts[from].addUtxo(new Utxo(tx.getId(), 1, from, ret.amount-amt));
	}

	console.log('tx:\n', tx.toHex());
	return { 'htlc': htlc, 'txhex': tx.toHex() };
    }
    
    print() {
	console.log('wallet:');
	for(var i in this.accounts) {
	    this.accounts[i].print();
	}
    }
}

class Utxo {
    
    static fromJson(obj) {
	return new Utxo(obj.txid, obj.vout, obj.owner, obj.amount);
    }
    
    constructor(txid, vout, owner, amount) {
	this.txid = txid;
	this.vout = vout;
	this.owner = owner;
	this.amount = amount;
    }
    
    id() {
	return this.txid+this.vout;
    }
    
    print() {
	console.log("utxo: %s %d %s %d", this.txid, this.vout, this.owner, this.amount);
    }
}

function foundTxVout(tx, addr)
{
    for(var i in tx['vout']) {
	if(addr == tx['vout'][i]['scriptPubKey']['addresses'][0]) {
	    return tx['vout'][i]['n'];
	}
    }
    console.log("foundTxVout failed");
    return 0;
}

async function createAccount() {
    const keyPair = btclib.ECPair.makeRandom({network: cnetwork});
    var pk = keyPair.publicKey.toString('hex');
    var sk = keyPair.privateKey.toString('hex');
    console.log('sk:', sk);
    console.log('pk:', pk);
    const { address } = btclib.payments.p2pkh({ pubkey: keyPair.publicKey, network:cnetwork });
    console.log("addr:", address);

    var amount = 0.1;
    var txid = await client.sendToAddress(address, amount);

    var tx = await client.getRawTransaction(txid, true);
    //console.log("tx:\n", JSON.stringify(tx, null, 4));

    var n = foundTxVout(tx, address);
    
    var account = new Account(pk, sk, address);
    account.addUtxo(new Utxo(txid, n, sk, amount*100000000));
    return account;
}

function createTransaction(sk, inCoins, addr, outamt) {
    const alice = sk2ecpair(sk);
    const txb = new btclib.TransactionBuilder(cnetwork);

    const { address } = btclib.payments.p2pkh({ pubkey: alice.publicKey, network:cnetwork });
    
    var inamt = 0;
    for(var i in inCoins) {
	inamt += inCoins[i].amount;
    }
    
    var changeamt = inamt-outamt-50000;
    
    txb.setVersion(1);
    
    for(var i in inCoins) {
	txb.addInput(inCoins[i].txid, inCoins[i].vout); 	
    }
    
    txb.addOutput(addr, outamt);
    
    if(changeamt > 1000) {
	txb.addOutput(address, changeamt);
    }
    
    for(var i in inCoins) {
	txb.sign(parseInt(i), alice);
    }
    
    return txb.build();
}

function hashtimelockcontract (hashx, redeemLockTimeStamp, destHash160Addr, revokerHash160Addr) {
    let redeemScript = btclib.script.compile([
        btclib.opcodes.OP_IF,
        btclib.opcodes.OP_SHA256,
        Buffer.from(hashx, 'hex'),
        btclib.opcodes.OP_EQUALVERIFY,
        btclib.opcodes.OP_DUP,
        btclib.opcodes.OP_HASH160,

        Buffer.from(destHash160Addr, 'hex'),//btclib.crypto.hash160(storeman.publicKey),
        btclib.opcodes.OP_ELSE,
        btclib.script.number.encode(redeemLockTimeStamp),
        btclib.opcodes.OP_CHECKLOCKTIMEVERIFY,
        btclib.opcodes.OP_DROP,
        btclib.opcodes.OP_DUP,
        btclib.opcodes.OP_HASH160,

        Buffer.from(revokerHash160Addr, 'hex'),
        btclib.opcodes.OP_ENDIF,
	
        btclib.opcodes.OP_EQUALVERIFY,
        btclib.opcodes.OP_CHECKSIG
    ])

    let addressPay = btclib.payments.p2sh({
        redeem: {output: redeemScript, network: cnetwork},
        network: cnetwork
    })
    
    let address = addressPay.address

    return {
        'p2sh': address,
        'hashx': hashx,
        'redeemLockTimeStamp': redeemLockTimeStamp,
        'redeemScript': redeemScript
    }
}

function sk2ecpair(sk) {
    var skb = Buffer.from(sk, 'hex');
    return btclib.ECPair.fromPrivateKey(skb, {network: cnetwork});
}

function ecpairTest() {
    var pk = '03a15d9642880e1ba6e25baf84e8c00f74090ec2aa4ea6ceeb1d99a7e1061145ff';
    var sk = '2786a71e5586f45f54653d6a9b1afd904b3ee09409bd41ae56da313ebb0d77f8';
    var skb = Buffer.from(sk, 'hex');
    var pair = btclib.ECPair.fromPrivateKey(skb);
    var pk1 = pair.publicKey.toString('hex');
    var sk1 = pair.privateKey.toString('hex');
    console.log('pk1:', pk1);
    console.log('sk1:', sk1);
}

function createRedeemHTLCTransaction(x, htlc, txid, from, to) {
    var txb = new btclib.TransactionBuilder(cnetwork);
    txb.setVersion(1);
    txb.addInput(txid, 0);
    
    txb.addOutput(to, 888);

    var frompair = sk2ecpair(from);
    
    let tx = txb.buildIncomplete();
    let sigHash = tx.hashForSignature(0, htlc.redeemScript, btclib.Transaction.SIGHASH_ALL);
    
    let redeemScriptSig = btclib.payments.p2sh({
        redeem: {
            input: btclib.script.compile([
                btclib.script.signature.encode(frompair.sign(sigHash), btclib.Transaction.SIGHASH_ALL),
                frompair.publicKey,
                Buffer.from(x, 'hex'),
                btclib.opcodes.OP_TRUE
            ]),
            output: redeemScript,
        },
        network: cnetwork
    }).input;
    
    tx.setInputScript(0, redeemScriptSig);

    return tx;
}

async function main() {
    
    var alice = await createAccount();
    var bob = await createAccount();
    var john = await createAccount();

    var wallet = new Wallet();
    wallet.addAccount(alice);
    wallet.addAccount(bob);
    
    console.log(JSON.stringify(wallet, null, 4));

    //var txhex = wallet.send2address(alice.id(), bob.addr, 8000);
    var x = Buffer.from(crypto.randomBytes(32))
    var ret = wallet.send2addressByHTLC(alice.id(), bob.pk, 8000, x.toString('hex'));

    var hex = await client.sendRawTransaction(ret.txhex);
    console.log('sendRawTransaction:', hex);
    
    console.log(JSON.stringify(wallet, null, 4));

    var htlc = ret.htlc;
    console.log(htlc);

    createRedeemHTLCTransaction(x.toString('hex'), htlc, txid, bob.id(), bob.addr);
    
    return;
    
    // var sk = 'ae7eb07bcfcba45838e2b3c4d3ae4743bfc0cb995b33ac378d4f329979b8a947';
    // var txid = 'fa99ef6908552104000dc93c194c484cca89aafef2736a4ea253472aacea1593';
    // var vout = 0;
    // var addr = 'mnVT56mVxPpEpsAGEiq517GHjxfm3H6ES8';
    // var txhex = createTransaction(sk, txid, vout, 10000000, addr, 12000);
    // console.log(txhex);
    // return;
    
    // var a1 = await createAccount();
    // var a2 = await createAccount();
    // console.log('-----------------------------');
    // console.log("a1", a1);
    // console.log("a2", a2);
    // return;
    
    //var data = fs.readFileSync('input.txt');
    //var fd = fs.openSync('a.txt', 'a');
    //fs.writeSync(fd, 'aaa\n');
    //fs.writeSync(fd, 'bbb\n');
    // var u1 = new Utxo("txid123", 1, 'jack0001');
    // var u2 = new Utxo("txid123", 2, 'jack0001');
    // var u3 = new Utxo("txid123", 3, 'jack0001');
    // var a1 = new Account("p001", "s001", "a001");
    // a1.addUtxo(u1);
    // a1.addUtxo(u2);
    // a1.addUtxo(u3);
    // var str = JSON.stringify(a1);
    // console.log(str);
    // var obj = JSON.parse(str);
    // var a2 = Account.fromJson(obj)
    // a2.print();
    // return;
    
    // var count = await client.getBlockCount();
    
    // var all = [];
    // all.push(getBlockTxs(100));
    // for(var i=count-1; i<count; ++i) {
    // 	all.push(getBlockTxs(i));
    // }
    
    // var vals = await Promise.all(all);
    // for(var val of vals) {
    // 	console.log(JSON.stringify(val, null, 4));
    // }

    // cnetwork = btclib.networks.regtest;
    
    // const keyPair = btclib.ECPair.makeRandom({network: cnetwork});
    // console.log('sk:', keyPair.privateKey.toString('hex'));
    // console.log('pk:', keyPair.publicKey.toString('hex'));
    // fs.writeSync(util.format('sk:%s', keyPair.privateKey.toString('hex')));
    // fs.writeSync(util.format('pk:%s', keyPair.publicKey.toString('hex')));
    
    // const { address } = btclib.payments.p2pkh({ pubkey: keyPair.publicKey, network:cnetwork });
    // console.log("addr:", address);
    // fs.writeSync(util.format('addr:%s', address));
    
    // var balance1 = await client.getBalance();
    // console.log("balance1: ", balance1);
    // var txid = await client.sendToAddress('muuXnS8T1mmiaLLcELFAup2tt5o2oXWYu8', 0.1);
                                           
    // console.log('txid: ', txid);
    // var balance2 = await client.getBalance();
    // console.log("balance2: ", balance2);

    // var tx = await client.getRawTransaction(txid, true);
    // console.log(JSON.stringify(tx, null, 4));
}

main();


// var Block = btclib.Block;

// var blockhex = '0000002059401153d3d7bff6787b3e0b21f9f8deee3cd17f065a3fd3b5a0b7cab20c34229b5df6c2b129a06a14555be5603775830309cd7ce1c838fda8a3a91388fe7803e172a45cffff7f200000000001020000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff0401640101ffffffff0200f2052a0100000017a914d69b630409eb86c2334257ec815088a5bea90440870000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf90120000000000000000000000000000000000000000000000000000000000000000000000000';

// async function getBlockTxs(height) {
//     var hash = await client.getBlockHash(height);
//     var block = await client.getBlock(hash);
//     var txs = [];
//     for(var tx of block.tx) {
// 	txs.push(client.getRawTransaction(tx, true));
//     }
//     var vals = await Promise.all(txs);
//     return vals;
// }

// var block = Block.fromHex(blockhex);

// console.log("block %s", block.getId().toString());

// console.log("block %s", block.getHash().toString('hex'));

// console.log("block transactions: ", block.transactions.length);

// for (var tx of block.transactions) {
//     console.log(tx);
// }

//-----------------------------------------------------------------

// var config = {
//     protocol: 'http',
//     user: '__cookie__',
//     pass: '1f3609530ad697640926c66da95e511740614f7e7cfeb99d5d37ee127ab8d57a',
//     host: '127.0.0.1',
//     port: '18443',
// };

// var rpc = new RpcClient(config);
// rpc.getblockcount((err, ret) => {
//     if(!err)
// 	console.log(ret);
//     else
// 	console.log(err);
// });

//-----------------------------------------------------------------

// const gi = require('node-gtk')
// Gtk = gi.require('Gtk', '3.0')
 
// gi.startLoop()
// Gtk.init()
 
// const win = new Gtk.Window();
// win.on('destroy', () => Gtk.mainQuit())
// win.on('delete-event', () => false)
 
// win.setDefaultSize(200, 80)
// win.add(new Gtk.Label({ label: 'Hello Gtk+' }))
 
// win.showAll();
// Gtk.main();

//-----------------------------------------------------------------

// const express = require('express')
// const app = express()
// app.get('/', (req, res) => res.send('Hello World!'))
// app.listen(3000, () => console.log('Example app listening on port 3000!'))

// var express = require("express");
// var app = express();
// app.use("/",express.static(__dirname + "/public"));
// app.listen(3000);

//-----------------------------------------------------------------

// const rocksdb = require('rocksdb-node');
// const db = rocksdb.open({create_if_missing: true}, '/home/lmf/github.com/palliums-developers/exchange-matchengine/match_engine/localdb');
// var i = 0;
// for(;;) {
//     var str = util.format('o%s', (i++).toString().padStart(8, '0'));
//     var val = db.get(str);
//     if(val == null) break;
//     console.log(str, ':', val);
// }
// console.log("---------------------------------------------");
// i = 0;
// for(;;) {
//     var str = util.format('t%s', (i++).toString().padStart(8, '0'));
//     var val = db.get(str);
//     if(val == null) break;
//     console.log(str, ':', val);
// }

//-----------------------------------------------------------------

// var SqlClient = require('mariasql');

// var c = new SqlClient({
//     host: '47.106.208.207',
//     user: 'root',
//     password: '1234qwer',
//     db: 'test'
// });

// var count = 0;

// async function get_order_count() {
//     return new Promise(function (resolve, reject) {
// 	c.query('SELECT COUNT(*) FROM exchange_order', function(err, rows) {
// 	    if (err)
// 		reject(err);
// 	    else
// 		resolve(parseInt(rows[0]['COUNT(*)']));
// 	});
//     });
// }

// async function do_query(str) {
//     return new Promise(function (resolve, reject) {
// 	c.query(str, function(err, rows) {
// 	    if (err)
// 		reject(err);
// 	    else
// 		resolve(rows);
// 	});
//     });
// }

// async function insert_orders(idx, cnt) {
//     var all = [];
//     for(var i=0; i<cnt; ++i) {
// 	var now = parseInt(Date.now()/1000);
// 	var str = util.format('INSERT INTO exchange_order VALUES (%d, 0, 1, 0, "0.00023746", 20, 0, "jack", %d, %d, 0, 0, -1 )'
//  			      , idx++, now, now+3600);
// 	console.log(str);
// 	all.push(await do_query(str));
// 	str = util.format('INSERT INTO exchange_order VALUES (%d, 1, 0, 0, "0.00023746", 20, 0, "jack", %d, %d, 0, 0, -1 )'
//  			      , idx++, now, now+3600);
// 	console.log(str);
// 	all.push(await do_query(str));
// 	str = util.format('INSERT INTO exchange_order VALUES (%d, 1, 2, 0, "0.00023746", 30, 0, "jack", %d, %d, 0, 0, -1 )'
//  			      , idx++, now, now+3600);
// 	console.log(str);
// 	all.push(await do_query(str));
// 	str = util.format('INSERT INTO exchange_order VALUES (%d, 2, 1, 0, "0.00023746", 30, 0, "jack", %d, %d, 0, 0, -1 )'
//  			      , idx++, now, now+3600);
// 	console.log(str);
// 	all.push(await do_query(str));
//     }
//     return all;
// }

// async function main1() {
//     count = await get_order_count();
//     console.log(count);
//     var orders = await insert_orders(count, 2);
//     console.log(orders);
//     c.end();
// }

// main1();

//-----------------------------------------------------------------

// var a = client.getBlockCount()
// a.then((value) => { console.log(value); })

//-----------------------------------------------------------------

// ./bitcoind -regtest -txindex -daemon
// ./bitcoin-cli -regtest getbalance
// cat ~/.bitcoin/regtest/.cookie
// curl --user __cookie__:1f3609530ad697640926c66da95e511740614f7e7cfeb99d5d37ee127ab8d57a --data-binary '{"jsonrpc": "1.0", "id":"curltest", "method": "getblockcount", "params": [] }' -H 'content-type: text/plain;' http://127.0.0.1:18443/

