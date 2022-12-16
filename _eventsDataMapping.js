

const mapFetchedEventsAssetsData = v => {
    return {
        id: parseInt(v.asset.id),
        token_id: v.asset.token_id,
        num_sales: parseInt(v.asset.num_sales),
        background_color: v.asset.background_color,
        image_url: v.asset.image_url,
        image_preview_url: v.asset.image_preview_url,
        image_thumbnail_url: v.asset.image_thumbnail_url,
        image_original_url: v.asset.image_original_url,
        animation_url: v.asset.animation_url,
        animation_original_url: v.asset.animation_original_url,
        name: v.asset.name,
        description: v.asset.description,
        external_link: v.asset.external_link,
        asset_contract: v.asset.asset_contract,
        permalink: v.asset.permalink,
        collection: v.asset.collection,
        decimals: parseInt(v.asset.decimals),
        token_metadata: v.asset.token_metadata,
        owner: v.asset.owner,
        sell_orders: v.asset.sell_orders ?? [],
        creator: v.asset.creator ?? {},
        traits: v.asset.traits ?? [],
        last_sale: v.asset.last_sale ?? null,
        top_bid: v.asset.top_bid ?? null,
        listing_date: v.asset.listing_date ?? null,
        is_presale: v.asset.is_presale ?? false,
        transfer_fee_payment_token: v.asset.transfer_fee_payment_token ?? null,
        transfer_fee: v.asset.transfer_fee ?? null
    }
};

const mapFetchedTransactionsData = (a, v) => {
    return {
        token_id: a.asset.token_id,
        address: a.contract_address,
        collection_name: a.collection_slug,
        transection: v
    };
};

exports.mapFetchedEventsAssetsData = mapFetchedEventsAssetsData
exports.mapFetchedTransactionsData = mapFetchedTransactionsData