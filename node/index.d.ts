/** Endpoint name routes refer to, e.g. `{ custom: { name: "pulsar", config: {...} } }`. */
export const ENDPOINT_NAME: "pulsar";

/**
 * Absolute path of the bundled plugin library for this platform.
 *
 * Throws if the package has no prebuild for the current platform.
 */
export function libraryPath(): string;

/**
 * Register the `pulsar` endpoint with mq-bridge.
 *
 * Call once, before starting any route that uses it; calling it again is a
 * no-op. Returns the registered endpoint name.
 */
export function register(): string;
