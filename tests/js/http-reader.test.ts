import { setImmediate } from "node:timers/promises";
import { tableFromIPC } from "apache-arrow";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import * as wasm from "../../pkg/node/parquet_wasm.js";
import {
  readExpectedArrowData,
  temporaryServer,
  testArrowTablesEqual,
} from "./utils.js";

describe("HTTP ParquetFile errors", () => {
  let server: Awaited<ReturnType<typeof temporaryServer>>;
  let url: string;

  beforeAll(async () => {
    server = await temporaryServer();
    url = `http://localhost:${server.addresses()[0].port}/2-partition-snappy.parquet`;
  });
  afterAll(async () => {
    await server?.close();
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function expectValidRead() {
    const file = await wasm.ParquetFile.fromUrl(url);
    try {
      const table = tableFromIPC((await file.read()).intoIPCStream());
      testArrowTablesEqual(readExpectedArrowData(), table);
    } finally {
      file.free();
    }
  }

  function injectFailure(failure: string) {
    const fetch = globalThis.fetch;
    return vi.spyOn(globalThis, "fetch").mockImplementation(async (...args) => {
      if (failure === "network") {
        throw new TypeError("Injected network failure");
      }
      const response = await fetch(...args);
      if (failure === "body") {
        response.arrayBuffer = async () => {
          throw new TypeError("Injected response body failure");
        };
        return response;
      }
      const corrupted = failure === "corrupt";
      const body = corrupted
        ? new Uint8Array(await response.arrayBuffer()).fill(0)
        : failure === "oversized"
          ? new Uint8Array((await response.arrayBuffer()).byteLength + 1)
          : null;
      const replacement = new Response(body, {
        status:
          failure === "truncated" || failure === "oversized" || corrupted
            ? 206
            : Number(failure),
      });
      // reqwest reads Response.url even for errors; synthetic Responses omit it.
      Object.defineProperty(replacement, "url", { value: response.url });
      return replacement;
    });
  }

  async function expectFailure(promise: Promise<unknown>, failure: string) {
    await expect(promise).rejects.toBeInstanceOf(Error);
    if (failure === "404" || failure === "503") {
      await expect(promise).rejects.toThrow(new RegExp(failure));
    }
  }

  it.each(["network", "404", "503", "body"])(
    "rejects metadata opening on %s failure and can open again",
    async (failure) => {
      const fetch = injectFailure(failure);
      await expectFailure(wasm.ParquetFile.fromUrl(url), failure);
      fetch.mockRestore();
      await expectValidRead();
    },
  );

  for (const method of ["read", "stream"] as const) {
    it.each([
      "network",
      "404",
      "503",
      "body",
      "truncated",
      "oversized",
      "corrupt",
    ])(
      `rejects ${method} on %s data failure and can read again`,
      async (failure) => {
        const file = await wasm.ParquetFile.fromUrl(url);
        try {
          const fetch = injectFailure(failure);
          if (method === "read") {
            await expectFailure(file.read(), failure);
          } else {
            const reader = (await file.stream()).getReader();
            try {
              await expectFailure(reader.read(), failure);
            } finally {
              reader.releaseLock();
            }
          }
          fetch.mockRestore();
          await expectValidRead();
        } finally {
          file.free();
        }
      },
    );
  }

  it.each([{ columns: ["missing"] }, { rowGroups: [2] }])(
    "rejects invalid stream options %j and can read again",
    async (options) => {
      const file = await wasm.ParquetFile.fromUrl(url);
      try {
        const reader = (await file.stream(options)).getReader();
        try {
          await expect(reader.read()).rejects.toThrow(
            options.columns ? /Column missing not found/ : /row group/i,
          );
        } finally {
          reader.releaseLock();
        }
        await expectValidRead();
      } finally {
        file.free();
      }
    },
  );

  it("rejects a later row group after emitting an earlier one", async () => {
    const file = await wasm.ParquetFile.fromUrl(url);
    const fetch = globalThis.fetch;
    let requests = 0;
    const mock = vi.spyOn(globalThis, "fetch").mockImplementation((...args) => {
      if (++requests === 2) {
        return Promise.reject(
          new TypeError("Injected later row group failure"),
        );
      }
      return fetch(...args);
    });
    try {
      const reader = (await file.stream({ concurrency: 1 })).getReader();
      try {
        const first = await reader.read();
        expect(first.done).toBe(false);
        const table = tableFromIPC(first.value.intoIPCStream());
        expect(table.getChild("str")!.toJSON()).toStrictEqual(["a", "b"]);
        await expect(reader.read()).rejects.toBeInstanceOf(Error);
        expect(requests).toBe(2);
      } finally {
        reader.releaseLock();
      }
      mock.mockRestore();
      await expectValidRead();
    } finally {
      file.free();
    }
  });

  it.each([
    [0, 1],
    [1, 0],
  ])(
    "preserves row group order %j and batch size with concurrent requests",
    async (...rowGroups) => {
      const file = await wasm.ParquetFile.fromUrl(url);
      const fetch = globalThis.fetch;
      const secondFinished = Promise.withResolvers<void>();
      const completionOrder: number[] = [];
      let requests = 0;
      vi.spyOn(globalThis, "fetch").mockImplementation(async (...args) => {
        const request = ++requests;
        const response = await fetch(...args);
        // Complete the second request first without timing-dependent sleeps.
        if (request === 1) {
          await secondFinished.promise;
        } else {
          secondFinished.resolve();
        }
        completionOrder.push(request);
        return response;
      });
      try {
        const reader = (
          await file.stream({ rowGroups, concurrency: 2, batchSize: 1 })
        ).getReader();
        const values: string[] = [];
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              break;
            }
            const table = tableFromIPC(value.intoIPCStream());
            expect(table.numRows).toBe(1);
            values.push(...table.getChild("str")!.toJSON());
          }
        } finally {
          reader.releaseLock();
        }
        expect(completionOrder).toStrictEqual([2, 1]);
        expect(values).toStrictEqual(
          rowGroups[0] === 0 ? ["a", "b", "c", "d"] : ["c", "d", "a", "b"],
        );
      } finally {
        file.free();
      }
    },
  );

  it.each(["cancellation", "failure"])(
    "allows an in-flight HTTP request to finish after stream %s",
    async (termination) => {
      const file = await wasm.ParquetFile.fromUrl(url);
      const fetch = globalThis.fetch;
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const bodyRead = Promise.withResolvers<void>();
      const concurrency = termination === "failure" ? 2 : 1;
      let requests = 0;
      const mock = vi
        .spyOn(globalThis, "fetch")
        .mockImplementation(async (...args) => {
          if (++requests === 1 && termination === "failure") {
            // Fail the first request only once the second is already in flight.
            await started.promise;
            throw new TypeError("Injected concurrent request failure");
          }
          const response = await fetch(...args);
          const arrayBuffer = response.arrayBuffer.bind(response);
          response.arrayBuffer = async () => {
            const bytes = await arrayBuffer();
            bodyRead.resolve();
            return bytes;
          };
          started.resolve();
          await release.promise;
          return response;
        });
      try {
        const reader = (await file.stream({ concurrency })).getReader();
        try {
          const pendingRead = reader.read();
          if (termination === "cancellation") {
            await started.promise;
            await reader.cancel();
            await expect(pendingRead).resolves.toMatchObject({ done: true });
          } else {
            await expect(pendingRead).rejects.toBeInstanceOf(Error);
          }
        } finally {
          reader.releaseLock();
        }
        expect(requests).toBe(concurrency);
        release.resolve();
        await bodyRead.promise;
        // Both termination paths drop receivers before outstanding HTTP tasks finish.
        await setImmediate();
        mock.mockRestore();
        await expectValidRead();
      } finally {
        release.resolve();
        file.free();
      }
    },
  );
});
