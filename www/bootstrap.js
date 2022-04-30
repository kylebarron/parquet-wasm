// A dependency graph that contains any wasm must all be imported
// asynchronously. This `bootstrap.js` file does the single async import, so
// that no one else needs to worry about it again.
// import("./index.js")
//   .catch(e => console.error("Error importing `index.js`:", e));
// import("./example.js")
//   .catch(e => console.error("Error importing `example.js`:", e));
import("./async_testing.js")
  .catch(e => console.error("Error importing `async_testing.js`:", e));
