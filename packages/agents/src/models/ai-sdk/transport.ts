import type { AISettings } from "../core/settings";
import {
  type Transport as CoreTransport,
  createTransport as createCoreTransport,
  errorFromGatewayEnvelope as coreErrorFromGatewayEnvelope,
  errorFromResponse as coreErrorFromResponse,
  unwrapEnvelope as coreUnwrapEnvelope
} from "../core/transport";
import { CloudflareAIError, toAISDKError } from "./errors";

export {
  definedHeaders,
  errorMessageFrom,
  headersToObject,
  isGatewayErrorEnvelope,
  type NativeRequest,
  type TransportRequest
} from "../core/transport";

/**
 * The typed AI SDK error for the gateway's own error envelope — a missing
 * gateway, an unpaid account — on the provider-native path.
 */
export function errorFromGatewayEnvelope(
  body: unknown,
  context: Parameters<typeof coreErrorFromGatewayEnvelope>[1]
): CloudflareAIError {
  return CloudflareAIError.fromCore(
    coreErrorFromGatewayEnvelope(body, context)
  );
}

/**
 * The transport this module uses: the run path for a model id, and the
 * provider-native path for a vendor's own model object, through the binding
 * or over HTTP.
 *
 * @experimental This surface is experimental and may change.
 */
export type Transport = CoreTransport;

/**
 * Unwraps the Cloudflare `{ success, result }` envelope, throwing an AI SDK
 * {@link CloudflareAIError} for a failed envelope.
 */
export function unwrapEnvelope(
  value: unknown,
  context?: Parameters<typeof coreUnwrapEnvelope>[1]
): unknown {
  try {
    return coreUnwrapEnvelope(value, context);
  } catch (error) {
    throw toAISDKError(error);
  }
}

/** The typed AI SDK error for a non-2xx response. */
export async function errorFromResponse(
  response: Response,
  context: Parameters<typeof coreErrorFromResponse>[1]
): Promise<CloudflareAIError> {
  return CloudflareAIError.fromCore(
    await coreErrorFromResponse(response, context)
  );
}

/**
 * The responses this module re-wrapped, so {@link CoreTransport.logIdFallback}
 * still reaches the response the binding recorded its log id against.
 */
const originalResponses = new WeakMap<Response, Response>();

/**
 * Lifts a failure that escapes a streamed body.
 *
 * `run()` resolves as soon as the response arrives, so a core error raised
 * later — a resume that runs out of reconnects, or a replay AI Gateway no
 * longer holds — is thrown while the caller reads the stream, long after the
 * `catch` in {@link createTransport} could see it. Reading the body through
 * this wrapper keeps the promise this module makes: every failure a caller
 * sees is the AI SDK error type, not the framework-neutral core one.
 *
 * The provider-native path is deliberately not wrapped. A vendor's own model
 * reads its own response, and its errors are its package's to raise.
 */
function liftBodyErrors(response: Response): Response {
  if (response.body === null) return response;
  const reader = response.body.getReader();
  const lifted = new ReadableStream<Uint8Array>({
    cancel(reason) {
      return reader.cancel(reason);
    },
    async pull(controller) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch (error) {
        controller.error(toAISDKError(error));
        return;
      }
      if (chunk.done) controller.close();
      else controller.enqueue(chunk.value);
    }
  });
  const wrapped = new Response(lifted, {
    headers: response.headers,
    status: response.status,
    statusText: response.statusText
  });
  originalResponses.set(wrapped, response);
  return wrapped;
}

/**
 * Builds the transport for a set of provider settings, with every failure
 * lifted into the AI SDK error type.
 *
 * @experimental This surface is experimental and may change.
 */
export function createTransport(settings: AISettings): Transport {
  const inner = createCoreTransport(settings);
  return {
    logIdFallback: (response) =>
      inner.logIdFallback(originalResponses.get(response) ?? response),
    async native(request) {
      try {
        return await inner.native(request);
      } catch (error) {
        throw toAISDKError(error);
      }
    },
    async run(request) {
      try {
        return liftBodyErrors(await inner.run(request));
      } catch (error) {
        throw toAISDKError(error);
      }
    },
    url: inner.url
  };
}
