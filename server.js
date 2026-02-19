const express = require("express");
const { Pool } = require("pg");
const fetch = require("node-fetch");
const { parse } = require("csv-parse/sync");

const app = express();
app.use(express.json());

function parseSheetDateToISO(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (!s) return null;

  // Already ISO
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // Common India format: DD/MM/YYYY or DD-MM-YYYY
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (m) {
    const dd = m[1].padStart(2, "0");
    const mm = m[2].padStart(2, "0");
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  // If unknown format, keep raw only
  return null;
}


/* --------------------------
   DATABASE CONNECTION (ONLY ONCE)
--------------------------- */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

/* --------------------------
   HEALTH CHECK
--------------------------- */
app.get("/", (req, res) => {
  res.send("Webhook server running");
});

/* --------------------------
   OPTIONAL: MANUAL INGEST ENDPOINT
--------------------------- */
app.post("/ingest-meta", async (req, res) => {
  try {
    const rows = req.body.rows;

    if (!rows || !Array.isArray(rows)) {
      return res.status(400).json({ error: "Invalid payload" });
    }

    for (const row of rows) {
      await pool.query(
        `INSERT INTO meta_ads_fact
        (date, ad_id, ad_name, ad_code, campaign_name,
         spend, impressions, clicks, ctr, cpc, cpm,
         purchases, purchase_value, roas)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (date, ad_id)
        DO UPDATE SET
          ad_name = EXCLUDED.ad_name,
          campaign_name = EXCLUDED.campaign_name,
          spend = EXCLUDED.spend,
          impressions = EXCLUDED.impressions,
          clicks = EXCLUDED.clicks,
          ctr = EXCLUDED.ctr,
          cpc = EXCLUDED.cpc,
          cpm = EXCLUDED.cpm,
          purchases = EXCLUDED.purchases,
          purchase_value = EXCLUDED.purchase_value,
          roas = EXCLUDED.roas`,
        row
      );
    }

    res.json({ success: true, inserted: rows.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

/* --------------------------
   DAILY META PULL + METADATA SYNC
--------------------------- */
app.get("/run-daily", async (req, res) => {
  if (req.query.secret !== process.env.CRON_SECRET) {
  return res.status(401).send("Unauthorized");
}

  try {
/* --------------------------
  1.  SYNC METADATA FROM GOOGLE SHEET (ALL HEADERS)
--------------------------- */
const sheetUrl =
  "https://docs.google.com/spreadsheets/d/1Uu2pnd-i6-PNh_YLeAFvQUu0AaVtwkBnVTLY4U5gKGM/export?format=csv&gid=970591175";

const sheetResponse = await fetch(sheetUrl);
const csvText = await sheetResponse.text();

const records = parse(csvText, {
  columns: true,
  skip_empty_lines: true,
  relax_column_count: true,
});

let metaRowsUpserted = 0;

for (const r of records) {
  // IMPORTANT: "Ad Code" is the primary key
  const adCodeRaw = (r["Ad Code"] || "").trim();
  const ad_code = adCodeRaw.toUpperCase().replace("_", "-");
  if (!ad_code) continue;

  // Clean up headers
  const metaDescriptor =
    (r["Meta Ad Descriptor "] || r["Meta Ad Descriptor"] || "").trim() || null;
   
   const dateRaw = (r["Date"] || "").trim() || null;
   const dateISO = parseSheetDateToISO(dateRaw);

  await pool.query(
    `
    INSERT INTO ad_metadata_dim (
      ad_code,
      month, date_raw, date, 
      creative_name, creative_link, product,
      funnel_level, ad_objective, creative_type_format, visual_style,
      content_pillar_bucket_narrative, hook, key_rtb_reason_to_buy,
      voice_over, primary_emotion_tone, language_captions_supers,
      offer, prices, season, production_team, created_by,
      starring_name_in_video_product_only,
      meta_ad_title, meta_ad_descriptor, live,
      updated_at
    )
    VALUES (
      $1,
      $2, $3,
      $4, $5, $6,
      $7, $8, $9, $10,
      $11, $12, $13,
      $14, $15, $16,
      $17, $18, $19, $20, $21,
      $22,
      $23, $24, $25, $26,
      CURRENT_TIMESTAMP
    )
    
    ON CONFLICT (ad_code)
    DO UPDATE SET
    month = EXCLUDED.month,
    date_raw = EXCLUDED.date_raw,
    date = EXCLUDED.date,
    creative_name = EXCLUDED.creative_name,
    creative_link = EXCLUDED.creative_link,
    product = EXCLUDED.product,
    funnel_level = EXCLUDED.funnel_level,
    ad_objective = EXCLUDED.ad_objective,
    creative_type_format = EXCLUDED.creative_type_format,
    visual_style = EXCLUDED.visual_style,
    content_pillar_bucket_narrative = EXCLUDED.content_pillar_bucket_narrative,
    hook = EXCLUDED.hook,
    key_rtb_reason_to_buy = EXCLUDED.key_rtb_reason_to_buy,
    voice_over = EXCLUDED.voice_over,
    primary_emotion_tone = EXCLUDED.primary_emotion_tone,
    language_captions_supers = EXCLUDED.language_captions_supers,
    offer = EXCLUDED.offer,
    prices = EXCLUDED.prices,
    season = EXCLUDED.season,
    production_team = EXCLUDED.production_team,
    created_by = EXCLUDED.created_by,
    starring_name_in_video_product_only = EXCLUDED.starring_name_in_video_product_only,
    meta_ad_title = EXCLUDED.meta_ad_title,
    meta_ad_descriptor = EXCLUDED.meta_ad_descriptor,
    live = EXCLUDED.live,
    updated_at = CURRENT_TIMESTAMP    `,
    [
      ad_code,
      (r["Month"] || "").trim() || null,
       dateRaw,
       dateISO,
      //(r["Date"] || "").trim() || null,

      (r["Creative Name"] || "").trim() || null,
      (r["Creative Link"] || "").trim() || null,
      (r["Product"] || "").trim() || null,

      (r["Funnel Level"] || "").trim() || null,
      (r["Ad Objective"] || "").trim() || null,
      (r["Creative Type (Format)"] || "").trim() || null,
      (r["Visual Style"] || "").trim() || null,

      (r["Content/ Pillar Bucket ( Narrative )"] || "").trim() || null,
      (r["Hook"] || "").trim() || null,
      (r["Key RTB (Reason to Buy)"] || "").trim() || null,

      (r["Voice Over"] || "").trim() || null,
      (r["Primary Emotion/Tone"] || "").trim() || null,
      (r["Language / Captions / Supers"] || "").trim() || null,

      (r["Offer"] || "").trim() || null,
      (r["Prices"] || "").trim() || null,
      (r["Season"] || "").trim() || null,
      (r["Production Team"] || "").trim() || null,
      (r["Created By"] || "").trim() || null,

      (r["Starring (Name of Person in Video / Product Only)"] || "").trim() || null,

      (r["Meta Ad Title"] || "").trim() || null,
      metaDescriptor,
      (r["Live"] || "").trim() || null,
    ]
  );

  metaRowsUpserted++;
}

    /* --------------------------
       2) PULL META (YESTERDAY)
    --------------------------- */
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split("T")[0];

    const accountId = process.env.META_AD_ACCOUNT_ID;
    const accessToken = process.env.META_ACCESS_TOKEN;

    const url =
      `https://graph.facebook.com/v19.0/${accountId}/insights` +
      `?level=ad` +
      `&fields=date_start,ad_id,ad_name,campaign_name,impressions,clicks,spend,actions,action_values` +
      `&time_range={'since':'${dateStr}','until':'${dateStr}'}` +
      `&access_token=${accessToken}`;

    const response = await fetch(url);
    const json = await response.json();

    if (!json.data) {
      console.error(json);
      return res.status(500).send("Meta API error");
    }

    let factRowsUpserted = 0;

    for (const row of json.data) {
      const impressions = parseInt(row.impressions || 0);
      const clicks = parseInt(row.clicks || 0);
      const spend = parseFloat(row.spend || 0);

      let purchases = 0;
      let purchase_value = 0;

      if (row.actions) {
        const purchaseAction = row.actions.find(
          (a) => a.action_type === "purchase"
        );
        if (purchaseAction) purchases = parseInt(purchaseAction.value);
      }

      if (row.action_values) {
        const purchaseValue = row.action_values.find(
          (a) => a.action_type === "purchase"
        );
        if (purchaseValue) purchase_value = parseFloat(purchaseValue.value);
      }

      const ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
      const cpc = clicks > 0 ? spend / clicks : 0;
      const cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
      const roas = spend > 0 ? purchase_value / spend : 0;

      const match = row.ad_name ? row.ad_name.match(/NK[-_]\d+/i) : null;
      const adCode = match ? match[0].toUpperCase().replace("_", "-") : null;

      await pool.query(
        `INSERT INTO meta_ads_fact (
          date, ad_id, ad_name, ad_code, campaign_name,
          spend, impressions, clicks, ctr, cpc, cpm,
          purchases, purchase_value, roas
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (date, ad_id)
        DO UPDATE SET
          ad_name = EXCLUDED.ad_name,
          campaign_name = EXCLUDED.campaign_name,
          spend = EXCLUDED.spend,
          impressions = EXCLUDED.impressions,
          clicks = EXCLUDED.clicks,
          ctr = EXCLUDED.ctr,
          cpc = EXCLUDED.cpc,
          cpm = EXCLUDED.cpm,
          purchases = EXCLUDED.purchases,
          purchase_value = EXCLUDED.purchase_value,
          roas = EXCLUDED.roas`,
        [
          row.date_start,
          row.ad_id,
          row.ad_name,
          adCode,
          row.campaign_name,
          spend,
          impressions,
          clicks,
          ctr,
          cpc,
          cpm,
          purchases,
          purchase_value,
          roas,
        ]
      );

      factRowsUpserted++;
    }

    res.send(
      `OK: metadata upserted=${metaRowsUpserted}, fact upserted=${factRowsUpserted} for ${dateStr}`
    );
  } catch (error) {
    console.error(error);
    res.status(500).send("Server error");
  }
});

//BACKFILL FOR DATES
app.get("/backfill", async (req, res) => {
  try {
    const start = req.query.start;  // format YYYY-MM-DD
    const end = req.query.end;      // format YYYY-MM-DD

    if (!start || !end) {
      return res.status(400).send("Provide ?start=YYYY-MM-DD&end=YYYY-MM-DD");
    }

    const accountId = process.env.META_AD_ACCOUNT_ID;
    const accessToken = process.env.META_ACCESS_TOKEN;

    let current = new Date(start);
    const endDate = new Date(end);

    let totalInserted = 0;

    while (current <= endDate) {
      const dateStr = current.toISOString().split("T")[0];
      console.log("Backfilling:", dateStr);

      const url =
        `https://graph.facebook.com/v19.0/${accountId}/insights` +
        `?level=ad` +
        `&fields=date_start,ad_id,ad_name,campaign_name,impressions,clicks,spend,actions,action_values` +
        `&time_range={'since':'${dateStr}','until':'${dateStr}'}` +
        `&access_token=${accessToken}`;

      const response = await fetch(url);
      const json = await response.json();

      if (json.data) {
        for (const row of json.data) {
          const impressions = parseInt(row.impressions || 0);
          const clicks = parseInt(row.clicks || 0);
          const spend = parseFloat(row.spend || 0);

          let purchases = 0;
          let purchase_value = 0;

          if (row.actions) {
            const purchaseAction = row.actions.find(a => a.action_type === "purchase");
            if (purchaseAction) purchases = parseInt(purchaseAction.value);
          }

          if (row.action_values) {
            const purchaseValue = row.action_values.find(a => a.action_type === "purchase");
            if (purchaseValue) purchase_value = parseFloat(purchaseValue.value);
          }

          const ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
          const cpc = clicks > 0 ? spend / clicks : 0;
          const cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
          const roas = spend > 0 ? purchase_value / spend : 0;

          const match = row.ad_name ? row.ad_name.match(/NK[-_]\d+/i) : null;
          const adCode = match ? match[0].toUpperCase().replace("_", "-") : null;

          await pool.query(
            `
            INSERT INTO meta_ads_fact (
              date, ad_id, ad_name, ad_code, campaign_name,
              spend, impressions, clicks, ctr, cpc, cpm,
              purchases, purchase_value, roas
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (date, ad_id)
            DO UPDATE SET
              ad_name = EXCLUDED.ad_name,
              campaign_name = EXCLUDED.campaign_name,
              spend = EXCLUDED.spend,
              impressions = EXCLUDED.impressions,
              clicks = EXCLUDED.clicks,
              ctr = EXCLUDED.ctr,
              cpc = EXCLUDED.cpc,
              cpm = EXCLUDED.cpm,
              purchases = EXCLUDED.purchases,
              purchase_value = EXCLUDED.purchase_value,
              roas = EXCLUDED.roas
            `,
            [
              row.date_start,
              row.ad_id,
              row.ad_name,
              adCode,
              row.campaign_name,
              spend,
              impressions,
              clicks,
              ctr,
              cpc,
              cpm,
              purchases,
              purchase_value,
              roas,
            ]
          );

          totalInserted++;
        }
      }

      current.setDate(current.getDate() + 1);
    }

    res.send(`Backfill complete. Rows processed: ${totalInserted}`);

  } catch (err) {
    console.error(err);
    res.status(500).send("Backfill failed");
  }
});


/* --------------------------
   START SERVER
--------------------------- */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
