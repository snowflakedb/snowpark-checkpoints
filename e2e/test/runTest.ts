import * as cp from 'child_process';
import * as path from 'path';
import {
  downloadAndUnzipVSCode,
  resolveCliArgsFromVSCodeExecutablePath,
  runTests
} from '@vscode/test-electron';

async function main() {
  try {
    const extensionDevelopmentPath = path.resolve(__dirname, '../../');
    const extensionTestsPath = path.resolve(__dirname, './suite/index');
    const vscodeExecutablePath = await downloadAndUnzipVSCode('1.78.0'); // TODO use ENV var
    const workspaceFolder = path.resolve(__dirname, '../sampleWorkspace');
    const launchArgs = [workspaceFolder]
    const [cliPath, ...args] = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath);

    // Use cp.spawn / cp.exec for custom setup
    cp.spawnSync(
      cliPath,
      [...args, '--install-extension', './extension/snowflake-vsc.vsix'], // TODO use ENV var
      {
        encoding: 'utf-8',
        stdio: 'inherit'
      }
    );

    // Run the extension test
    await runTests({
      // Use the specified `code` executable
      vscodeExecutablePath,
      extensionDevelopmentPath,
      extensionTestsPath,
      launchArgs
    });
  } catch (err) {
    console.error('Failed to run tests');
    process.exit(1);
  }
}

main();
