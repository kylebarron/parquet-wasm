import '@loaders.gl/polyfills';
// import 'node-fetch';
// import 'whatwg-fetch';
// import {fetch as fetchPolyfill} from 'whatwg-fetch'

// fetchPolyfill.XMLHttpRequest;
// console.log(XMLHttpRequest)

import { server } from './mocks/server';

// Establish API mocking before all tests.
server.listen();
server.printHandlers()

// import './arrow1';
// import './arrow2';
import './arrow2-async';

server.close();
