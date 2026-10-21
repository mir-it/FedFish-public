# FedFish — Fishbowl Shipping Automation

Desktop app that pulls packed shipments from Fishbowl Server and sends them to
Synapse as outbound orders.

> **Public sanitized copy.** This repository has a clean history and contains
> **no passwords, tokens, or private certificates**. Copy `.env.example` to
> `.env` and fill in your own credentials locally. Never commit `.env`.

## Setup

```
pip install -r requirements.txt
cp .env.example .env
# fill in Fishbowl host/creds and Synapse credentials
python app.py
```

## Flow

1. **Login** — enter Fishbowl host/port/username/password (prefilled from `.env`).
2. **Shipments** — table of packed shipments (statusId=20) for the configured
   location group.
3. **Pallet detail** — select a shipment to see its pallets and line items.
4. **Create Synapse Order** — builds the Synapse payload from the shipment and
   sends it to Synapse. You can toggle "Show Synapse response" to inspect the
   full request/response JSON.

## Notes

- Location group filter and packed-status ID live at the top of `queries.py`.
- Synapse line conversions and lot requirements are controlled by `.env` values.
- Optional FedFish Central upload is documented in `CENTRAL_SYNC.md`. The TLS
  CA file (`central-ca.pem`) is **not** included here; obtain it from your
  private install if you use Central.
- Package for Windows with `pyinstaller --onefile --windowed app.py`.
