# Frontend

Static radiation map frontend served and built with Vite.

## Local development

```bash
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5500
```

The middleware WebSocket URL is configured with `VITE_WS_URL`. Copy
`.env.example` to `.env` and pick the endpoint you need:

```bash
cp .env.example .env
```

- Local testing (without using Kafka): `ws://localhost:8080/testing` this simply replays
  `measurements-out-short.csv`. This is the default in `.env.example`.
- Live Kafka stream: `ws://localhost:8080/ws` which needs `docker compose up` and
  the producer running.
- Deployment later over HTTPS will be: `wss://our-host/ws`.

The producer settings tab uses `VITE_PRODUCER_API_URL`. For local development the
frontend defaults to:

```text
http://localhost:8042/producer
```

If the producer settings API runs on a different host or port, add this to `.env`:

```bash
VITE_PRODUCER_API_URL=http://localhost:8042/producer
```

> DON'T FORGET TO restart `npm run dev` after editing `.env` (because Vite only reads it at startup).

## Interactive viewport

The map at the moment is viewport-driven. I have done this so that we can send the data to the middleware and receive the points that are only in the middleware. Whenever we pan or zoom, it sends its current bounds like this: 
(`{type:"viewport", north, east, south, west, zoom}`) to the middleware, which sends
streams back only the points inside that region (each tagged with its H3 cell).
We remove the arkers that scroll off screen in the browser, so the dataset never 
piles up. All filtering happens server-side and as it's written in our task, the frontend does no computation.


## Production build

```bash
npm run build
```

The deployable static files are generated in:

```text
dist/
```