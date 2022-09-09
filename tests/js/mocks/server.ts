import { setupServer } from 'msw/node'
import { handlers } from './handlers'

console.log('hellow orld from server');

// This configures a request mocking server with the given request handlers.
export const server = setupServer(...handlers)
