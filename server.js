const express = require("express");
const { Pool } = require("pg");

const app = express();
app.use(express.json());
app.get('/', (req, res) => {
  res.send('Webhook server running');
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

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
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
        row
      );
    }

    res.json({ success: true, inserted: rows.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

//Daily sync
const fetch = require('node-fetch');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get('/run-daily', async (req, res) => {
  try {

    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];

    console.log("Pulling Meta data for:", dateStr);

    const accountId = process.env.META_AD_ACCOUNT_ID;
    const accessToken = process.env.META_ACCESS_TOKEN;

    const url = `https://graph.facebook.com/v19.0/${accountId}/insights` +
      `?level=ad` +
      `&fields=date_start,ad_id,ad_name,impressions,clicks,spend,actions` +
      `&time_range={'since':'${dateStr}','until':'${dateStr}'}` +
      `&access_token=${accessToken}`;

    const response = await fetch(url);
    const data = await response.json();

    if (!data.data) {
      console.error(data);
      return res.status(500).send("Meta API error");
    }

    for (const row of data.data) {

      const match = row.ad_name ? row.ad_name.match(/NK[-_]\d+/i) : null;
      const adCode = match ? match[0].toUpperCase().replace("_", "-") : null;

      const impressions = parseInt(row.impressions || 0);
      const clicks = parseInt(row.clicks || 0);
      const spend = parseFloat(row.spend || 0);

      await pool.query(
        `
        INSERT INTO meta_ads_fact (
          date,
          ad_id,
          ad_name,
          ad_code,
          impressions,
          clicks,
          spend
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (date, ad_id)
        DO UPDATE SET
          impressions = EXCLUDED.impressions,
          clicks = EXCLUDED.clicks,
          spend = EXCLUDED.spend
        `,
        [
          row.date_start,
          row.ad_id,
          row.ad_name,
          adCode,
          impressions,
          clicks,
          spend
        ]
      );
    }

    res.send(`Inserted/updated ${data.data.length} ads for ${dateStr}`);

  } catch (error) {
    console.error(error);
    res.status(500).send("Server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
