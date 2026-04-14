# Griffin Direct-Debit Events (SSE) - Plan Upgrade/Downgrade

This document describes how a React client can consume **Server-Sent Events** from the remittance backend when Griffin webhooks are processed (payments, admissions, settlements, and optional subscription snapshot on settlement).

## Endpoint

| Item | Value |
|------|--------|
| Method | `GET` |
| Path | `/api/v1/wallets/griffin/events/stream` |
| Full URL | `{API_BASE_URL}/api/v1/wallets/griffin/events/stream` |

Example: `https://api.example.com/api/v1/wallets/griffin/events/stream`

## Authentication and access

- **`Authorization: Bearer <access_token>`** (Laravel Sanctum personal access token or equivalent).
- The route uses **`auth:sanctum`** and **`check.status.api`**: the user must be authenticated **and** account-verified (`tv` on the user model). Unverified users receive an API error instead of a stream.

**Important:** The browser’s native **`EventSource` API does not support custom headers.** If you only have a Bearer token (no cookie session), use **`fetch()`** with a readable stream and parse SSE frames manually (see below). If your app uses **SPA Sanctum with cookies** on the same site, `EventSource` may work with `credentials: 'include'` depending on CORS — prefer `fetch` + Bearer if in doubt.

## Wire format (SSE)

The server sends [SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html) frames:

- **`id:`** — Monotonic numeric id (database row id). Send this back as **`Last-Event-ID`** on reconnect to resume after the last delivered event.
- **`event:`** — Logical event name (see [Event names](#event-names)).
- **`data:`** — Single JSON object as a string (parse with `JSON.parse`).
- **Comments** — Lines like `: heartbeat` (ignore for application logic; used to keep connections alive).

Example raw chunk:

```text
id: 42
event: griffin.transaction.settled
data: {"paymentUuid":"...","transactionId":1,...}

```

The stream runs for up to **30 minutes** per connection; the client should **reconnect** and send **`Last-Event-ID`** with the last received `id`.

## Event names

| `event` value | When it is emitted |
|---------------|--------------------|
| `griffin.admission.created` | Griffin admission created (direct debit / payment initiation flow). |
| `griffin.admission.updated` | Admission status updated. |
| `griffin.payment.created` | Payment record created (e.g. pending). |
| `griffin.transaction.settled` | Account transaction settled (balance change applied). |

Parse `data` as JSON for every event.

## Payload shapes

### `griffin.transaction.settled`

```ts
type GriffinTransactionSettledPayload = {
  paymentUuid: string;
  transactionId: number;
  status: string; // e.g. "settled"
  currency: string | null;
  amount: string | number | null;
  direction: string | null;
  walletId: string;
  subscriptionCredit: boolean;
  creditAmount: number | null;
  occurredAt: string; // ISO 8601
  /** Present only when the backend attached a subscription snapshot (see below). */
  subscription?: GriffinSubscriptionSseSnapshot;
};

type GriffinSubscriptionSseSnapshot = {
  subscriptionId: number;
  planId: number;
  planKey: string | null;
  status: 'active' | 'pending_payment' | 'suspended' | 'cancelled';
  amountOwed: number;
};
```

**`subscription` is optional.** It appears when:

1. This settlement was handled as a **wallet credit** that participates in the subscription pipeline (`subscriptionCredit === true`), and  
2. The user has a **subscription row** at snapshot time.

Use **`subscription`** for quick UI hints; **always refetch** your canonical subscription API (e.g. `GET /api/v2/subscription`) to confirm entitlements and full plan details.

### Other events

Admission and payment-created payloads are documented in the backend in `GriffinSsePublisher` (`griffin.admission.*`, `griffin.payment.created`). Shape is stable and filtered server-side.

## React: recommended approach

1. **One long-lived connection** per logged-in, verified user (e.g. mount in a layout or provider when the dashboard loads).
2. **`AbortController`** to cancel the stream on logout or unmount.
3. **Reconnect** with exponential backoff on network errors; send **`Last-Event-ID`** from the last `id` you parsed.
4. On `griffin.transaction.settled`, if `subscription` is present or `subscriptionCredit` is true, **invalidate** TanStack Query keys for subscription/plan data.

### TypeScript types (shared)

```ts
// types/griffinSse.ts
export type GriffinSseEventName =
  | 'griffin.admission.created'
  | 'griffin.admission.updated'
  | 'griffin.payment.created'
  | 'griffin.transaction.settled';

export type GriffinSubscriptionSseSnapshot = {
  subscriptionId: number;
  planId: number;
  planKey: string | null;
  status: string;
  amountOwed: number;
};

export type GriffinTransactionSettledPayload = {
  paymentUuid: string;
  transactionId: number;
  status: string;
  currency: string | null;
  amount: string | number | null;
  direction: string | null;
  walletId: string;
  subscriptionCredit: boolean;
  creditAmount: number | null;
  occurredAt: string;
  subscription?: GriffinSubscriptionSseSnapshot;
};
```

### Parsing SSE from `fetch` (Bearer token)

```ts
async function* parseSseStream(
  body: ReadableStream<Uint8Array> | null,
): AsyncGenerator<{ id?: string; event: string; data: string }> {
  if (!body) return;
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let idx: number;
    while ((idx = buffer.indexOf('\n\n')) !== -1) {
      const raw = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);

      const lines = raw.split('\n');
      let id: string | undefined;
      let event = 'message';
      const dataLines: string[] = [];

      for (const line of lines) {
        if (line.startsWith(':')) continue; // comment / heartbeat
        if (line.startsWith('id:')) id = line.slice(3).trim();
        else if (line.startsWith('event:')) event = line.slice(6).trim();
        else if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
      }

      if (dataLines.length) {
        yield { id, event, data: dataLines.join('\n') };
      }
    }
  }
}
```

### Hook sketch (`useGriffinSse`)

Place `parseSseStream` in e.g. `src/lib/griffinSseStream.ts` and import it into the hook file.

```tsx
import { useEffect, useRef } from 'react';
import { parseSseStream } from '@/lib/griffinSseStream';
import type { GriffinTransactionSettledPayload } from '@/types/griffinSse';

type Options = {
  apiBaseUrl: string;
  getAccessToken: () => string | null;
  onTransactionSettled?: (payload: GriffinTransactionSettledPayload) => void;
  onError?: (error: unknown) => void;
};

export function useGriffinSse({
  apiBaseUrl,
  getAccessToken,
  onTransactionSettled,
  onError,
}: Options) {
  const lastIdRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    const ac = new AbortController();

    async function run() {
      const token = getAccessToken();
      if (!token) return;

      const url = `${apiBaseUrl.replace(/\/$/, '')}/api/v1/wallets/griffin/events/stream`;
      const headers: HeadersInit = {
        Authorization: `Bearer ${token}`,
        Accept: 'text/event-stream',
      };
      if (lastIdRef.current) {
        (headers as Record<string, string>)['Last-Event-ID'] = lastIdRef.current;
      }

      try {
        const res = await fetch(url, {
          method: 'GET',
          headers,
          signal: ac.signal,
        });

        if (!res.ok) {
          onError?.(new Error(`SSE ${res.status}`));
          return;
        }

        for await (const frame of parseSseStream(res.body)) {
          if (frame.id) lastIdRef.current = frame.id;

          if (frame.event === 'griffin.transaction.settled') {
            const payload = JSON.parse(frame.data) as GriffinTransactionSettledPayload;
            onTransactionSettled?.(payload);
          }
          // handle other event names as needed
        }
      } catch (e) {
        if ((e as Error).name !== 'AbortError') onError?.(e);
      }
    }

    void run();
    return () => ac.abort();
  }, [apiBaseUrl, getAccessToken, onTransactionSettled, onError]);
}
```

**Note:** The dependency array above is minimal; in production you may want a **stable** `onTransactionSettled` via `useCallback` or a ref to avoid reconnecting on every parent render. Reconnect logic with backoff is left as an exercise.

### TanStack Query invalidation

```ts
import { useQueryClient } from '@tanstack/react-query';

// Inside onTransactionSettled:
queryClient.invalidateQueries({ queryKey: ['subscription'] });
queryClient.invalidateQueries({ queryKey: ['plans'] });
```

Adjust keys to match your Orval-generated query key factories.

## Mobile(Fluter)
- Flutter HTTP Server-Sent Events (SSE) client library for receive real-time updates from the server
- [https://pub.dev/packages/flutter_http_sse](https://pub.dev/packages/flutter_http_sse)

## CORS

Ensure the API allows:

- `GET` to the stream URL from your frontend origin.
- Headers: `Authorization`, `Last-Event-ID`, `Accept`.
- If using credentials, configure `Access-Control-Allow-Credentials` appropriately.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| 401 | Missing/invalid token or expired session. |
| Error / `unverified` style message | User not verified for API (`check.status.api`). |
| No events | No Griffin activity for this user yet; stream still sends heartbeats. |
| Duplicate events after reconnect | Ensure `Last-Event-ID` matches the **last processed** `id`. |

