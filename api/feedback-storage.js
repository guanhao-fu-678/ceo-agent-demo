import { get as blobGet, list as blobList, put as blobPut } from "@vercel/blob";
import { get as tigrisGet, list as tigrisList, put as tigrisPut } from "@tigrisdata/storage";

const REQUIRED_TIGRIS_ENV = [
  "TIGRIS_STORAGE_ACCESS_KEY_ID",
  "TIGRIS_STORAGE_SECRET_ACCESS_KEY",
  "TIGRIS_STORAGE_BUCKET",
];

export const EVENT_LIST_KEY = "feedback-spike-events";

export function tokenPathSegment(value) {
  return encodeURIComponent(String(value || "").trim()).replaceAll("%", "_");
}

function hasBlobConnectionEnv() {
  return Boolean(process.env.BLOB_STORE_ID || process.env.BLOB_WEBHOOK_PUBLIC_KEY);
}

function hasBlobWriteToken() {
  return Boolean(process.env.BLOB_READ_WRITE_TOKEN);
}

function hasTigrisConfig() {
  const missing = REQUIRED_TIGRIS_ENV.filter((key) => !process.env[key]);
  return missing.length === 0;
}

export function storageBackend() {
  if (hasBlobWriteToken()) {
    return "vercel_blob";
  }
  if (hasTigrisConfig()) {
    return "tigris";
  }
  return "";
}

export function storageConfigError() {
  if (storageBackend()) {
    return "";
  }
  if (hasBlobConnectionEnv() && !hasBlobWriteToken()) {
    return "blob_not_configured:BLOB_READ_WRITE_TOKEN";
  }
  const missingTigris = REQUIRED_TIGRIS_ENV.filter((key) => !process.env[key]);
  return `storage_not_configured:BLOB_READ_WRITE_TOKEN or ${missingTigris.join(",")}`;
}

function throwIfStorageError(result, action) {
  if (result && result.error) {
    throw new Error(`Tigris ${action} failed: ${result.error.message}`);
  }
  return result.data;
}

async function putBlobJson(path, payload) {
  await blobPut(path, payload, {
    access: "private",
    allowOverwrite: true,
    contentType: "application/json",
  });
}

async function putTigrisJson(path, payload) {
  throwIfStorageError(
    await tigrisPut(path, payload, {
      allowOverwrite: true,
      contentType: "application/json",
    }),
    "put",
  );
}

export async function persistFeedbackEvent(event) {
  const configError = storageConfigError();
  if (configError) {
    return { persisted: false, error: configError };
  }

  const backend = storageBackend();
  const payload = JSON.stringify(event);
  const putJson = backend === "vercel_blob" ? putBlobJson : putTigrisJson;

  await putJson(`${EVENT_LIST_KEY}/${event.key}.json`, payload);
  if (event.feedback_token) {
    await putJson(
      `${EVENT_LIST_KEY}/by-token/${tokenPathSegment(event.feedback_token)}/${event.key}.json`,
      payload,
    );
  }
  return { persisted: true, error: "", backend };
}

export async function listFeedbackEventPaths(prefix, limit) {
  const configError = storageConfigError();
  if (configError) {
    throw new Error(configError);
  }
  if (storageBackend() === "vercel_blob") {
    const data = await blobList({ limit, prefix });
    return [...(data.blobs || [])]
      .sort(
        (left, right) =>
          new Date(right.uploadedAt || 0).getTime() -
          new Date(left.uploadedAt || 0).getTime(),
      )
      .map((item) => item.pathname);
  }
  const data = throwIfStorageError(
    await tigrisList({
      limit,
      prefix,
    }),
    "list",
  );
  return [...(data.items || [])]
    .sort(
      (left, right) =>
        new Date(right.lastModified).getTime() - new Date(left.lastModified).getTime(),
    )
    .map((item) => item.name);
}

export async function readFeedbackEvent(path) {
  if (storageBackend() === "vercel_blob") {
    const result = await blobGet(path, { access: "private" });
    if (!result || result.statusCode !== 200 || !result.stream) {
      throw new Error(`Vercel Blob get failed: ${result ? result.statusCode : "not_found"}`);
    }
    return JSON.parse(await new Response(result.stream).text());
  }
  const data = throwIfStorageError(await tigrisGet(path, "string"), "get");
  return JSON.parse(data);
}
