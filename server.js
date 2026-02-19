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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
