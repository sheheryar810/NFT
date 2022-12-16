"use strict";

// Get dependencies
const http = require('http');
const express = require("express");
const cors = require("cors");
const app = express();

// Body Parser Configuration
app.use(express.json({ limit: "250mb" }));
app.use(cors());

const { checkIfTableExists, loadDataToBigQuery, deleteDataFromTables } = require('./bigQuery');
const { updateAssetsData, fetchAssets, fetchTransactionData, loadDataToBQuery } = require('./newLogic');

// Get our API routes
app.get('/health', (req, res) => {
    res.status(200).json({ message: "Its Working", datetime: Date.now() });
});

// ================== Cron Job ==================
var cron = require('node-cron');
// cron.schedule('*/45 * * * *', () => {
//     deleteDataFromCollection()
//         .then(result => {
//             console.log({ result, datetime: Date.now() })
//             fetchTransaction();
//             fetchDataFromOpenSea('lostpoets', '28000', true, false);
//             // fetchDataFromOpenSea('m', '28000', true, false);
//         })
//     console.log('running a task every hour');
// });

// cron.schedule('30 */1 * * *', () => {
//     deleteDataFromCollection()
//         .then(result => {
//             console.log({ result, datetime: Date.now() })
//             insertDataInBigQuery()
//         })
// });
// ================== Cron Job ==================

const axios = require('axios');

const { MongoClient } = require('mongodb');
const uri = "mongodb://127.0.0.1:27017/";
const client = new MongoClient(uri);

// =========================== Run Cron - Start ===========================
// cron.schedule('*/45 * * * *', () => {
//     fetchAssets('lostpoets', '28000')
//     fetchTransactionData()
// });
app.get("/runCronForMongo", async (req, res) => {
    // fetchAssets('lostpoets', '28000')
    // fetchAssets('m', '28000')
    // fetchTransactionData()

// =========================== Update/Insert Traits Data to BigQuery - Start ===========================

    updateAssetsData('lostpoets', '28000')

// =========================== Update/Insert Traits Data to BigQuery - End ===========================
});

// cron.schedule('*/30 * * * *', () => { loadDataToBQuery() });
app.get("/runCronForBQuery", async (req, res) => { loadDataToBQuery() });
// =========================== Run Cron - End ===========================

app.get('/insertData', async (req, res) => {
    await client.connect();

    let url = `Link`;
    axios({ method: 'get', url })
        .then((response) => {
            let assets = response.data.assets;
            var dbo = client.db("lostpoet");
            dbo.collection("assets").insertMany(assets, function (err, mongoRes) {
                if (err) {
                    console.log("DB Error : ", err)
                } else {
                    client.close();
                    res.status(200).json({ Message: "Data has been inserted Successfully" })
                }
            });
        })
        .catch((error) => {
            res.status(200).json({ Error: error })
        });
});

app.get('/insertData2', async (req, res) => {
    let offset = 0;
    let total = 0;
    await client.connect();
    while (offset < collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 5000));
        let url = `Link`;
        axios({ method: 'get', url })
            .then((response) => {
                let assets = response.data.assets;
                console.log(assets.length);
                total += assets.length;
                console.log(total);
                assets.forEach(element => {
                    console.log(element.token_id);
                });

                var dbo = client.db("lostpoets-1");

                // dbo.collection("assets").deleteMany({'collection.name':'merge.'}).then(res => {
                //     console.log(res);
                //     client.close();
                // })

                dbo.collection("assets").insertMany(assets, function (err, mongoRes) {
                    if (err) {
                        console.log("DB Error : ", err)
                    } else {
                        if (offset >= collectionLimit)
                            client.close();
                        console.log("Data inserted Successfully: " + total);
                    }
                });
            })
            .catch((error) => {
                console.log(error);
                return 0;
            });
        offset += 50;
    }
    console.log('after while');
});

app.get('/deleteData', async (req, res) => {
    await client.connect();
    var dbo = client.db("lostpoets-1");
    dbo.collection("assets").deleteMany({ 'collection.name': 'LOSTPOETS' }).then(response => {
        console.log(response);
        client.close();
        res.json(response)
    })
})

app.get('/insertDataBase', async (req, res) => {
    let collectionLimit = 28000;
    let offset = 20;
    let total = 0;

    await client.connect();
    while (offset <= collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 500));
        console.log("I : ", (offset - 20), ' ', "offset : ", offset)

        let tokenIds = '';
        for (let i = (offset - 20); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        axios({ method: 'get', url })
            .then(async (response) => {
                let assets = response.data.assets;
                if (assets.length > 0) {
                    total += assets.length;
                    var dbo = client.db("lostpoets-1");
                    await dbo.collection("assets").insertMany(assets, function (err, mongoRes) {
                        if (err) {
                            console.log("DB Error : ", err)
                        } else {
                            console.log("Data inserted Successfully ", total)
                        }
                    });
                }
            })
            .catch((error) => {
                // console.log(error)
                // res.status(200).json({ Error: error })
                // return 0;
            });
        offset += 20;
    }
})

app.get('/onRequestData', async (req, res) => {
    let collectionLimit = 100;
    let offset = 20;
    let total = 0;

    await client.connect();
    while (offset <= collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 500));
        console.log("I : ", (offset - 20), ' ', "offset : ", offset)

        let tokenIds = '';
        for (let i = (offset - 20); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        axios({ method: 'get', url })
            .then(async (response) => {
                let assets = response.data.assets;
                if (assets.length > 0) {
                    total += assets.length;
                    var dbo = client.db("lostpoets");
                    await dbo.collection("requested_data").insertMany(assets, function (err, mongoRes) {
                        if (err) {
                            console.log("DB Error : ", err)
                        } else {
                            console.log("Data inserted Successfully ", total)
                        }
                    });
                }
            })
            .catch((error) => {
                // console.log(error)
                // res.status(200).json({ Error: error })
                // return 0;
            });
        offset += 20;
    }
})

// =========================== Delete Collection - Start ===========================
app.get('/deleteCollection', async (req, res) => {
    deleteDataFromCollection()
        .then(result => { res.json(result) })
})

async function deleteDataFromCollection() {
    // deleteDataFromTables('assets')
    // deleteDataFromTables('traits')
    // deleteDataFromTables('sell_orders')
    await client.connect();
    return new Promise((resolve, reject) => {
        var dbo = client.db("lostpoets-1");
        dbo.collection("assets").deleteMany({})
            .then(responseAssets => {
                dbo.collection("traits").deleteMany({})
                    .then(responseTraits => {
                        dbo.collection("sell_orders").deleteMany({})
                            .then(responseSellOrder => {
                                dbo.collection("transaction").deleteMany({})
                                    .then(responseTransection => {
                                        resolve({ responseAssets, responseTraits, responseSellOrder, responseTransection })
                                    });
                            });
                    });
            });
    })
}
// =========================== Delete Collection - End ===========================

app.post('/insertData3', async (req, res) => {
    const { collectionName, collectionLimit, separateTrait, etherscane } = req.body;
    fetchDataFromOpenSea(collectionName, collectionLimit, separateTrait, false);
})

async function fetchDataFromOpenSea(collectionName, collectionLimit, separateTrait, etherscane) {
    // console.log(collectionName, " / ", collectionLimit, " / ", separateTrait, " / ", etherscane)
    let offset = 0
    etherscane == true ? offset = 5 : offset = 20;
    let total = 0;

    await client.connect();
    while (offset <= collectionLimit) {
        etherscane
            ? await new Promise(resolve => setTimeout(resolve, 5000))
            : await new Promise(resolve => setTimeout(resolve, 2000));

        // console.log("I : ", (offset - 20), ' ', "offset : ", offset)

        let tokenIds = '';
        for (let i = (offset - 20); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        // sdk['getting-assets']({ order_direction: 'desc', offset: offset, limit: '20' })
        //     .then(response => console.log(response))
        //     .catch(err => console.error(err));

        axios({ method: 'get', url, headers: { 'Accept': 'application/json', 'x-api-key': 'Your-Key-Here' } })
            .then(async (response) => {
                let assets = response.data.assets;
                if (assets.length > 0) {
                    total += assets.length;
                    var dbo = client.db("lostpoets-1");

                    if (etherscane) {
                        getEtherScanRecord(assets)
                            .then(async (resTransAssets) => {
                                await dbo.collection("assets").insertMany(resTransAssets, function (err, mongoRes) {
                                    if (err) {
                                        console.log({ "DB Error": err, datetime: Date.now() })
                                    } else {
                                        console.log({ "Data inserted Successfully": total, datetime: Date.now() })
                                        if (separateTrait)
                                            insertTraitsBid(dbo, assets)
                                    }
                                })
                            })
                    } else {
                        // await dbo.collection("assets").insertMany(assets, function (err, mongoRes) {
                        //     if (err) {
                        //         console.log({ "DB Error": err, datetime: Date.now() })
                        //     } else {
                        //         // console.log({ "Data inserted Successfully": total, datetime: Date.now() })
                        //         if (separateTrait)
                        insertTraitsBid(dbo, assets)
                        //     }
                        // });
                    }
                }
            })
            .catch((error) => {
                console.log({ "Open Sea Error ": error, datetime: Date.now() })
                return 0;
            });
        offset += 20;
    }
}

function insertTraitsBid(dbo, assets) {
    return new Promise((resolve, reject) => {
        assets.forEach(async (assetData, index) => {
            if (assetData.traits != null && assetData.traits.length > 0) {
                await assetData.traits.forEach(async traitsData => {
                    traitsData.collection_name = assetData.collection.name;
                    traitsData.token_id = assetData.token_id;
                    traitsData.value = String(assetData.value) == "undefined" ? null : String(assetData.value);
                    // loadDataToBigQuery(traitsData, "traits")
                    await dbo.collection("traits").insertOne(traitsData, (err, mongoRes) => { });
                })
            }

            if (assetData.sell_orders != null && assetData.sell_orders.length > 0) {
                await assetData.sell_orders.forEach(async sellOrderData => {
                    sellOrderData.collection_name = assetData.collection.name;
                    sellOrderData.token_id = assetData.token_id;
                    // loadDataToBigQuery(sellOrderData, "sell_orders")
                    await dbo.collection("sell_orders").insertOne(sellOrderData, (err, mongoRes) => { });
                })
            }

            assetData.timestamp = Date.now()
            await dbo.collection("assets").insertOne(assetData, (err, mongoRes) => { });

            // delete assetData['_id'];
            // delete assetData['sell_orders'];
            // delete assetData['traits'];
            // loadDataToBigQuery(assetData, "assets")

            if (assets.length == (index + 1))
                resolve()
        })
    })
}

app.get('/insertData4', async (req, res) => {
    let collectionLimit = 20;
    let offset = 20;
    let total = 0;

    await client.connect();
    while (offset <= collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 300));

        // console.log("I : ", (offset - 20), ' ', "offset : ", offset)

        let tokenIds = '';
        for (let i = (offset - 20); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        axios({ method: 'get', url })
            .then(async (response) => {
                let assets = response.data.assets;
                if (assets.length > 0) {
                    total += assets.length;
                    var dbo = client.db("lostpoets");

                    makeBatch(assets)
                        .then(result => {
                            let batch_0 = result[0], batch_1 = result[1], batch_2 = result[2], batch_3 = result[3]

                            let _0 = getEtherScanRecord(batch_0)
                                .then(async (resTransAssets) => {
                                    await dbo.collection("assets1").insertMany(resTransAssets, function (err, mongoRes) { })
                                })

                            let _1 = getEtherScanRecord(batch_1)
                                .then(async (resTransAssets) => {
                                    await dbo.collection("assets1").insertMany(resTransAssets, function (err, mongoRes) { })
                                })

                            let _2 = getEtherScanRecord(batch_2)
                                .then(async (resTransAssets) => {
                                    await dbo.collection("assets1").insertMany(resTransAssets, function (err, mongoRes) { })
                                })

                            let _3 = getEtherScanRecord(batch_3)
                                .then(async (resTransAssets) => {
                                    await dbo.collection("assets1").insertMany(resTransAssets, function (err, mongoRes) { })
                                })

                            Promise.all([_0, _1, _2, _3]).then((values) => {
                                console.log("Data inserted Successfully ", total)
                            });
                        })

                    // await dbInsertion(assets)
                    //     .then(async (resTransAssets) => {
                    //         await dbo.collection("assets1").insertMany(resTransAssets, function (err, mongoRes) {
                    //             if (err) {
                    //                 console.log("DB Error : ", err)
                    //             } else {
                    //                 console.log("Data inserted Successfully ", total)
                    //             }
                    //         });
                    //     })

                }
            })
            .catch((error) => {
                res.status(200).json({ Error: error })
                return 0;
            });
        offset += 20;
    }
});

function makeBatch(assets) {
    return new Promise((resolve, reject) => {
        let smallArr = [];
        let bigArr = [];
        assets.forEach(async (element, index) => {
            if (((index + 1) % 5) == 0) {
                smallArr.push(element)
                bigArr.push(smallArr)
                smallArr = [];
            } else {
                smallArr.push(element)
            }

            if (assets.length == (index + 1))
                resolve(bigArr)
        })
    })
}

// =========================== Delete Trasection - Start ===========================
app.get('/deleteTransection', async (req, res) => {
    deleteDataFromTransection()
        .then(result => { res.json(result) })
})

async function deleteDataFromTransection() {
    await client.connect();
    return new Promise((resolve, reject) => {
        var dbo = client.db("lostpoets-1");
        dbo.collection("transection").deleteMany({})
            .then(responseTransection => {
                resolve({ responseTransection })
            });
    })
}
// =========================== Delete Collection - End ===========================

// =========================== Etherscane - Start ===========================
app.post('/etherscaneData', async (req, res) => {
    const { collectionName, collectionLimit } = req.body
    // let collectionName = 'lostpoets';
    let offset = 9;
    let total = 0;
    // let collectionLimit = 5000;

    await client.connect();
    while (offset <= collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 5000));

        console.log("CollectionName : ", collectionName, " I : ", (offset - 9), " Offset : ", offset)

        let tokenIds = '';
        for (let i = (offset - 9); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        axios({ method: 'get', url })
            .then(async (response) => {
                let assets = response.data.assets;
                if (assets.length > 0) {
                    total += assets.length;
                    var dbo = client.db("lostpoets-1");

                    assets.forEach(async (element) => {
                        await requestURL(element.asset_contract.address)
                            .then(async (transection) => {
                                let obj = {
                                    token_id: element.token_id,
                                    address: element.asset_contract.address,
                                    collection_name: element.collection.name,
                                    transection: transection
                                };

                                // if (transection.length > 0) loadDataToBigQuery(transection, 'transaction')
                                await dbo.collection("transection").insertOne(obj, function (err, mongoRes) { })
                            })
                            .catch((error) => { })
                    })
                }
            })
            .catch((error) => { return 0 });
        offset += 9;
    }
})

function requestURL(contractaddress) {
    return new Promise(async (resolve, reject) => {
        let url = `Link`;

        axios({ method: 'get', url })
            .then(trasnsectionData => {
                trasnsectionData.data.result.length > 0
                    ? trasnsectionData.data.result != 'Max rate limit reached'
                        ? resolve(trasnsectionData.data.result)
                        : resolve('')
                    : resolve('')
            })
            .catch((error) => reject(error));
    })
}
// =========================== Etherscane - End ===========================
app.post('/insertDataInBigQuery', async (req, res) => {
    insertDataInBigQuery()
})

async function insertDataInBigQuery() {
    let limit = 20000;
    let offset = 10;

    await client.connect();
    while (offset <= limit) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        let skip = (offset - 10)
        console.log("Skip : ", skip, " offset : ", offset)

        var dbo = client.db("lostpoets-1");
        await dbo.collection("transaction").find({}).skip(skip).limit(10).toArray(function (err, resTransection) {
            if (resTransection.length > 0)
                resTransection.forEach(async (transectionData) => {
                    delete transectionData['_id'];
                    delete transectionData['timestamp'];
                    await loadDataToBigQuery(transectionData)
                })
        })
        offset += 10;
    }
}

app.post('/createBigQueryTable', async (req, res) => {
    // await checkIfTableExists("assets");
    // await checkIfTableExists("traits");
    // await checkIfTableExists("sell_orders");
    // await checkIfTableExists("transaction");
})

// =========================== Insert Data in Big Query from Mongo ===========================

// =========================== Contract_Address - Start ===========================
app.get("/contractaddress", async (req, res) => {
    fetchTransaction();
});

async function fetchTransaction() {
    let page = 1;
    await client.connect();
    var dbo = client.db("lostpoets-1");
    let urlDesc = `Link`;
    let urlAsc = `Link`;


    await axios.get(urlDesc)
        .then(async (trasnsectionData) => {
            if (trasnsectionData.data.result) {
                if (trasnsectionData.data.result.length > 0) {
                    await dbo.collection("transection").insertMany(trasnsectionData.data.result, function (err, mongoRes) { });
                }
            }
        })
        .catch((error) => console.log('axios error', error));
    await axios.get(urlAsc)
        .then(async (trasnsectionData) => {
            if (trasnsectionData.data.result) {
                if (trasnsectionData.data.result.length > 0) {
                    await dbo.collection("transection").insertMany(trasnsectionData.data.result, function (err, mongoRes) { });
                }
            }
        })
        .catch((error) => console.log('axios error', error));
}
// =========================== Contract_Address - Start ===========================

const server = http.createServer(app);
server.listen(3000, () => console.log(`API running on Server:3000`));
