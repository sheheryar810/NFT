"use strict";

// Get dependencies
const http = require('http');
const express = require("express");
const cors = require("cors");
const app = express();

// Body Parser Configuration
app.use(express.json({ limit: "250mb" }));
app.use(cors());

const axios = require('axios');

const { MongoClient } = require('mongodb');

const uri = "mongodb://127.0.0.1:27017/";
const client = new MongoClient(uri);

const { loadDataToBigQuery } = require('./bigQuery');
const { mapFetchedEventsAssetsData } = require('./_eventsDataMapping')

// ============== Per min Cron Job ===============
var cron = require('node-cron');
// cron.schedule('*/1 * * * *', () => {
//     fetchEvents();
//     console.log({ message: 'Per Minute Job Completed!', datetime: Date.now() });
// });
// ================== Cron Job ===================

// ====================== Fetch Event Collection - Start ===========================
app.get('/fetchEvents', async (req, res) => {
    await fetchEvents();
    return res.json({ message: 'Job Completed!' });
})

const fetchEvents = async () => {

    const MS_PER_MINUTE = 60000;
    let url = 'Link';
    let occurred_before = (new Date().getTime())
    let occurred_after = (occurred_before - MS_PER_MINUTE)

    let response = await axios.get(url, {
        params: { event_type: 'successful', only_opensea: false, occurred_before, occurred_after },
        headers: { 'Accept': 'application/json', 'x-api-key': 'Your-Key-Here' },
    });

    await client.connect();
    var dbo = client.db("lostpoets-1");
    if (response.data.asset_events.length > 0) {
        dbo.collection("assets").insertMany(response.data.asset_events, function (err, mongoRes) { });
        response.data.asset_events.forEach(async assetsData => {
            await loadDataToBigQuery(mapFetchedEventsAssetsData(assetsData), 'assets');
        })
    }
}

const server = http.createServer(app);
server.listen(3002, () => console.log(`API running on Server:3002`));