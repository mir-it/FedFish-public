# FedFish Central

Every FedFish PC can upload each Synapse send to **FedFish Central**, one shared history on your
office network. Central is a separate small server (not included in this public repo). A dashboard
on the Central PC can list every FedFish PC by name and block one.

Uploads never hold up shipping. FedFish saves each send to a local queue
(`.fedfish-central/outbox.sqlite3`) and uploads it in the background, retrying every 30 seconds while
FedFish is open and again at the next launch. If Central is off, sends wait in the queue and catch up later.

This public copy defaults to `CENTRAL_URL=off`. It also does **not** include `central-ca.pem`
(the certificate used to trust Central). Add that file locally if you enable Central.

## What gets recorded

- **Every send:** SO number, the full payload, the Synapse response, `succeeded` or `failed`, this PC's
  name, Windows user, computer name and time.
- **Past orders, once per PC:** the first time a PC starts with Central set up, FedFish imports its existing
  `synapse_last_send_map.txt`. That file only kept the last attempt per SO and has no send times.
  It never imports again on that PC.

## Add a FedFish PC

There are no keys. A PC is identified only by its name.

1. Put `central-ca.pem` next to the FedFish app (from your private Central install).
2. In that PC's `.env`, set `CENTRAL_URL` to your Central HTTPS URL, and optionally
   `CENTRAL_CLIENT_NAME` to the name it should show up as (e.g. the employee who uses it).
   Blank name uses the Windows computer name. Give every PC a different name.
3. Restart FedFish. Check with `python central_sync.py`: `pending` should reach 0 and
   `past_orders_imported_at` should be set.

## Block a PC

On the Central dashboard, click **Block** next to a PC name. While blocked, Central refuses that name.
The PC keeps shipping normally; its sends wait in its queue and upload after it's unblocked.

## Settings (`.env`)

| Setting | Meaning |
|---|---|
| `CENTRAL_CLIENT_NAME` | Name this PC appears as in Central. Defaults to the Windows computer name. |
| `CENTRAL_URL` | Central HTTPS URL, or `off` to disable uploads (default in this public copy). |
| `CENTRAL_CA_FILE` | Optional. Defaults to `central-ca.pem` in the FedFish folder. |

## Tests

`python -B -m unittest test_central_sync` (if the test file is present). Set `FEDFISH_CENTRAL_SOURCE`
to your Central `server.py` to also run against a real temporary receiver.
