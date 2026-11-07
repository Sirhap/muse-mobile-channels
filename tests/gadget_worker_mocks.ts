/** Keep worker transport mocks shared while the real MuseTurn stays loaded. */
import { mock } from 'bun:test';
import { join } from 'node:path';

const reference = join(import.meta.dir, '..', 'reference', 'muse-client', 'src');
let connectWorker: () => unknown = () => {
  throw new Error('configure the worker transport before running a case');
};

/** Select the fake connection for one worker invocation. */
export function configureWorkerMock(connect: () => unknown): void {
  connectWorker = connect;
}

mock.module(join(reference, 'client.js'), () => ({
  MuseClient: class {
    static async connect() {
      return connectWorker();
    }
  },
}));
mock.module(join(reference, 'credentials.js'), () => ({
  loadCredentials: async () => ({}),
  saveCredentials: async () => {},
}));
