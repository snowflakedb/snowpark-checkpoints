import * as cp from "child_process";
import * as path from "path";
import { config } from "dotenv";
import { downloadAndUnzipVSCode, resolveCliArgsFromVSCodeExecutablePath, runTests } from "@vscode/test-electron";

async function main() {
  try {
    const vscodeVersion = process.env.VSCODE_VERSION;
    const extensionInstaller = process.env.EXTENSION_INSTALLER_PATH ?? "";
    const testingWorkspace = process.env.TESTING_WORKSPACE ?? "";
    const extensionDevelopmentPath = path.resolve(__dirname, "../../");
    const extensionTestsPath = path.resolve(__dirname, "./suite/index");
    const vscodeExecutablePath = await downloadAndUnzipVSCode(vscodeVersion);
    const workspaceFolder = path.resolve(__dirname, `../${testingWorkspace}`);
    const launchArgs = [workspaceFolder];
    const [cliPath, ...args] = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath);

    // Use cp.spawn / cp.exec for custom setup
    cp.spawnSync(cliPath, [...args, "--install-extension", extensionInstaller], {
      encoding: "utf-8",
      stdio: "inherit",
    });

    // Run the extension test
    await runTests({
      // Use the specified `code` executable
      vscodeExecutablePath,
      extensionDevelopmentPath,
      extensionTestsPath,
      launchArgs,
    });
  } catch (err) {
    console.error("Failed to run tests");
    process.exit(1);
  }
}

config({ path: path.resolve(__dirname, "../.env.test") });
main();
