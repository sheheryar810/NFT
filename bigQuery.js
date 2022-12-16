const { BigQuery } = require('@google-cloud/bigquery');
const { private_key, client_email, project_id } = require('./lost-poets-project-ed0d1994c0d0.json');
const { dataSetId,
    Assets_Tables, assets_tableId, merge_assets_tableId,
    Sell_Orders_Tables, sell_orders_tableId, merge_traits_tableId,
    Traits_Tables, traits_tableId, merge_sell_orders_tableId,
    Transaction_Tables, transaction_tableId
} = require('./bigQuery.json');

const bigQuery = new BigQuery({
    projectId: project_id,
    credentials: {
        client_email: client_email,
        private_key: private_key,
    },
});

const loadDataToBigQuery = async (jsonData, type) => {
    return new Promise((resolve, reject) => {
        switch (type) {
            case "assets":
                bigQuery
                    .dataset(dataSetId)
                    .table(assets_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Assets : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Assets BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            case "traits":
                bigQuery
                    .dataset(dataSetId)
                    .table(traits_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Traits : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Traits BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            case "sell_orders":
                bigQuery
                    .dataset(dataSetId)
                    .table(sell_orders_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Sell Orders : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Sell Orders BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            case "merge_assets":
                bigQuery
                    .dataset(dataSetId)
                    .table(merge_assets_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Merge Assets : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Merge Assets BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            case "merge_traits":
                bigQuery
                    .dataset(dataSetId)
                    .table(merge_traits_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Merge Traits : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Merge Traits BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            case "merge_sell_orders":
                bigQuery
                    .dataset(dataSetId)
                    .table(merge_sell_orders_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Merge Sell Orders : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Merge Sell Orders BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;

            default:
                bigQuery
                    .dataset(dataSetId)
                    .table(transaction_tableId)
                    .insert(jsonData)
                    .then((data) => { console.log(`Transaction : Big Query Data Enter Successfully `, Date.now()) })
                    .catch((err) => console.log(`Transaction BQ Error: ${err} + ${JSON.stringify(jsonData)}`));
                break;
        }
    })
};

const checkIfTableExists = async (type) => {
    const dataset = bigQuery.dataset(dataSetId);
    let table = '';
    let tableId = '';
    switch (type) {
        case "assets":
            table = dataset.table(assets_tableId);
            tableId = assets_tableId;
            break;

        case "traits":
            table = dataset.table(traits_tableId);
            tableId = traits_tableId;
            break;

        case "sell_orders":
            table = dataset.table(sell_orders_tableId);
            tableId = sell_orders_tableId;
            break;

        default:
            table = dataset.table(transaction_tableId);
            tableId = transaction_tableId;
            break;
    }

    try {
        const data = await table.get();
        const apiResponse = data[1];
        console.log(`apiResponse: ${JSON.stringify(apiResponse, undefined, 3)}`);
    } catch (e) {
        if (e.code === 404) {
            console.log(`${e.message} >>> Creating table: ${tableId}`);
            await createBigQueryTablePartitioned(type, tableId);
        }
    }
};

const createBigQueryTablePartitioned = async (type, tableId) => {
    let options = '';
    switch (type) {
        case "assets":
            options = { schema: Assets_Tables, location: 'US' };
            break;

        case "traits":
            options = { schema: Traits_Tables, location: 'US' };
            break;

        case "sell_orders":
            options = { schema: Sell_Orders_Tables, location: 'US' };
            break;

        default:
            options = { schema: Transaction_Tables, location: 'US' };
            break;
    }

    // Create a new table in the dataset
    const [table] = await bigQuery.dataset(dataSetId).createTable(tableId, options);
    console.log(`Table ${table.id} created: `);
    return table.id;
};

module.exports = {
    loadDataToBigQuery,
    checkIfTableExists
};
