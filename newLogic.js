const axios = require('axios');
const { MongoClient, Transaction } = require('mongodb');
const uri = "mongodb://35.237.69.55:27017/";
const client = new MongoClient(uri);

const { BigQuery } = require('@google-cloud/bigquery');
const { private_key, client_email, project_id } = require('./lost-poets-project-ed0d1994c0d0.json');

const bigQuery = new BigQuery({
    projectId: project_id,
    credentials: {
        client_email: client_email,
        private_key: private_key,
    },
});

const { loadDataToBigQuery } = require('./bigQuery');

exports.fetchAssets = async (collectionName, collectionLimit) => {
    let offset = 20;

    await client.connect();
    while (offset <= collectionLimit) {
        await new Promise(resolve => setTimeout(resolve, 2000));
        let tokenIds = '';
        for (let i = (offset - 20); i <= offset; i++)
            tokenIds != '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i

        let url = `Link`;

        axios({ method: 'get', url, headers: { 'Accept': 'application/json', 'x-api-key': 'Your-Key-Here' } })
            .then(async (response) => {
                let assets = response.data.assets;
                var dbo = client.db("lostpoets-1");
                await dbo.collection("assets-1").insertMany(assets, (err, mongoRes) => { });
            })
            .catch((error) => { console.log("error : ", url) });
        offset += 20;
    }
}

exports.fetchTransactionData = async () => {
    await client.connect();
    var dbo = client.db("lostpoets-1");
    let urlDesc = `Link`;
    let urlAsc = `Link`;

    await axios.get(urlDesc)
        .then(async (trasnsectionData) => {
            if (trasnsectionData.data.result) {
                if (trasnsectionData.data.result.length > 0) {
                    trasnsectionData.data.result.forEach(async trasnsectionElements => {
                        trasnsectionElements.timestamp = Date.now();
                        let transectionQuery = await dbo.collection("transaction").findOne({ $and: [{ blockNumber: trasnsectionElements.blockNumber }, { tokenID: trasnsectionElements.tokenID }] });
                        if (transectionQuery) {
                            await dbo.collection("transaction").deleteOne({ $and: [{ blockNumber: trasnsectionElements.blockNumber }, { tokenID: trasnsectionElements.tokenID }] });
                            await dbo.collection("transaction").insertOne(trasnsectionElements, (err, mongoRes) => { });
                        }
                        else {
                            await dbo.collection("transaction").insertOne(trasnsectionElements, (err, mongoRes) => { });
                        }
                    });
                }
            }
        })
        .catch((error) => console.log('axios error', error));

    await axios.get(urlAsc)
        .then(async (trasnsectionData) => {
            if (trasnsectionData.data.result) {
                if (trasnsectionData.data.result.length > 0) {
                    trasnsectionData.data.result.forEach(async trasnsectionElements => {
                        trasnsectionElements.timestamp = Date.now();
                        let transectionQuery = await dbo.collection("transaction").findOne({ $and: [{ blockNumber: trasnsectionElements.blockNumber }, { tokenID: trasnsectionElements.tokenID }] });
                        if (transectionQuery) {
                            await dbo.collection("transaction").deleteOne({ $and: [{ blockNumber: trasnsectionElements.blockNumber }, { tokenID: trasnsectionElements.tokenID }] });
                            await dbo.collection("transaction").insertOne(trasnsectionElements, (err, mongoRes) => { });
                        }
                        else {
                            await dbo.collection("transaction").insertOne(trasnsectionElements, (err, mongoRes) => { });
                        }
                    });
                }
            }
        })
        .catch((error) => console.log('axios error', error));
}

exports.sendDataToBQuery = async () => {
    let limit = 30000;
    let offset = 10;

    await client.connect();
    var dbo = client.db("lostpoets-1");

    // let oldTime = Math.floor(new Date(new Date().getTime() - (60 * 60000)));
    // await dbo.collection("transaction-1").find().toArray(async function (err, resTransection) {
    //     if (resTransection.length > 0) separateTransactionData(resTransection);
    // });

    while (offset <= limit) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        let skip = (offset - 10)
        console.log("Skip : ", skip, " offset : ", offset)

        await dbo.collection("assets-1").find().skip(skip).limit(10).toArray(async function (err, resAsset) {
            if (resAsset.length > 0) separateAssetsData(resAsset);
        });
        offset += 10;
    }
}

async function separateTransactionData(resTransection) {
    let offset = 10;
    let totalLength = resTransection.length;

    while (offset <= totalLength) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        let skip = (offset - 10);
        let newArr = resTransection.slice(skip, offset)

        newArr.forEach(async (transectionData) => {
            transectionData.tokenName = transectionData.tokenName.replace(/\s+/g, '').toUpperCase();
            delete transectionData['_id'];
            delete transectionData['timestamp'];
            await loadDataToBigQuery(transectionData)
        })
        offset += 10;
    }
}

async function separateAssetsData(assets) {
    let offset = 10;
    let totalLength = assets.length;

    while (offset <= totalLength) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        let skip = (offset - 10);
        let newArr = assets.slice(skip, offset)

        newArr.forEach(async (assetData) => {
            if (assetData.traits != null && assetData.traits.length > 0) {
                await assetData.traits.forEach(async traitsData => {
                    traitsData.collection_name = assetData.collection.name;
                    traitsData.token_id = assetData.token_id;
                    // loadDataToBigQuery(traitsData, "traits")
                    loadDataToBigQuery(traitsData, "merge_traits")
                })
            }

            if (assetData.sell_orders != null && assetData.sell_orders.length > 0) {
                await assetData.sell_orders.forEach(async sellOrderData => {
                    sellOrderData.collection_name = assetData.collection.name;
                    sellOrderData.token_id = assetData.token_id;
                    // loadDataToBigQuery(sellOrderData, "sell_orders")
                    loadDataToBigQuery(sellOrderData, "merge_sell_orders")
                })
            }

            delete assetData['_id'];
            delete assetData['sell_orders'];
            delete assetData['traits'];
            delete assetData['timestamp'];
            // loadDataToBigQuery(assetData, "assets")
            loadDataToBigQuery(assetData, "merge_assets")
        })

        offset += 10;
    }
}

exports.updateAssetsData = async (collectionName, collectionLimit) => {
    try {
        let offset = 20;
        await client.connect();

        while (offset <= collectionLimit) {
            let tokenIds = '';
            for (let i = (offset - 20); i <= offset; i++)
                tokenIds !== '' ? tokenIds = tokenIds.concat('&token_ids=' + i) : tokenIds = 'token_ids=' + i;

            let url = `Link`;
            try {
                const response = await axios({ method: 'get', url, headers: { 'Accept': 'application/json', 'x-api-key': 'Your-Key-Here' } });
                let assets = response.data.assets;

                if (assets.length > 0) {
                    const dbo = client.db("lostpoets-1");
                    for (let assetData of assets) {
                        assetData.timestamp = Date.now();
                        let assetQuery = await dbo.collection("assets").findOne({ $and: [{ id: assetData.id }, { token_id: assetData.token_id }] });
                        if (assetQuery) {
                            dbo.collection("assets").updateOne({ $and: [{ id: assetData.id }, { token_id: assetData.token_id }] }, { $set: { traits: assetData.traits } }, function (err, res) {
                                if (err) throw err;
                            });
                            await sendTraitsBigQuery(assetData);
                        } else {
                            console.log("New Asset inserted at token_id: ", assetData.token_id);
                            await dbo.collection("assets").insertOne(assetData, (err, mongoRes) => { });
                        }
                    }
                }
            } catch (e) {
                console.log("error : ", url);
            }
            offset += 20;
        }
    } catch (error) {
        console.log("ERROR: UpdateAssetsData => ", error.message);
    }
};

async function sendTraitsBigQuery(asset) {
    await client.connect();
    const dbo = client.db("lostpoets-1");
    try {
        let assetsRow = await dbo.collection("assets").findOne({ token_id: asset.token_id });
        let assetRow = assetsRow.traits;

        for (let elementsRow of assetRow) {
            elementsRow.collection_name = asset.collection.name;
            elementsRow.token_id = asset.token_id;

            // await new Promise(resolve => setTimeout(resolve, 2000));
            let query = `Select * from lost-poets-project.lostpoet_mongo_atlas.new_traits where token_id = '${asset.token_id}' and  trait_type = '${elementsRow.trait_type}' and value = '${elementsRow.value}'`;
            let options = { query: query, location: 'US', };
            let [job] = await bigQuery.createQueryJob(options);
            const [rows] = await job.getQueryResults();
            if (rows.length > 0) {
                if (JSON.stringify(elementsRow) != JSON.stringify(rows[0])) {
                    // await new Promise(resolve => setTimeout(resolve, 2000));
                    let updateQuery = `Update lost-poets-project.lostpoet_mongo_atlas.new_traits set trait_count=${elementsRow.trait_count} where token_id = '${asset.token_id}' and  trait_type = '${elementsRow.trait_type}' and value = '${elementsRow.value}'`;
                    let updateOptions = { query: updateQuery, location: 'US' };
                    let [updateJob] = await bigQuery.createQueryJob(updateOptions);
                    const updateRows = await updateJob.getQueryResults();
                    console.log("Before: ", JSON.stringify(rows[0]), "After: ", JSON.stringify(elementsRow));
                } else {
                    console.log("No changes on token_id: ", asset.token_id);
                }
            }
            else {
                console.log("New trait inserted on token_id: ", asset.token_id);
                loadDataToBigQuery(elementsRow, 'traits');
            }
        }
    } catch (error) {
        console.log("Error: ", error);
    }
}

async function seperateTraitsMongoDB(asset) {
    let query = { $and: [{ id: asset.id }, { token_id: asset.token_id }] };
    await client.connect();
    var dbo = client.db("lostpoets-1");

    let assetJson = asset.traits;
    let newSellOrderJson = asset.sell_orders;
    let traitArray = await dbo.collection("assets").find(query).toArray();
    if (traitArray) {
        await traitArray.forEach(async traitData => {
            let traitJson = traitData.traits;
            if (JSON.stringify(assetJson) != JSON.stringify(traitJson)) {
                dbo.collection("assets").updateOne(query, { $set: { traits: assetJson } }, function (err, res) {
                    if (err) throw err;
                });
            }
            let sellJson = traitData.sell_orders;
            if (JSON.stringify(newSellOrderJson) != JSON.stringify(sellJson)) {
                dbo.collection("assets").updateOne(query, { $set: { sell_orders: newSellOrderJson } }, function (err, res) {
                    if (err) throw err;
                });
            }
        });
    }
}
