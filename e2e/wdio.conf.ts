import type { Options } from "@wdio/types";
import { VSCodeOptions } from "wdio-vscode-service/dist/types";
import { join } from "path";
import { config as envConfig } from "dotenv";

envConfig({ path: join(__dirname, ".env.test") });

const VSCodeVersion = process.env.VSCODE_VERSION || "stable";
const TestsWorkspace = process.env.TESTING_WORKSPACE || __dirname;
const ExtensionPath = process.env.EXTENSION_PATH || __dirname;
const ExtensionInstallerPath = process.env.EXTENSION_INSTALLER_PATH || __dirname;

export const config: Options.Testrunner = {
  runner: "local",
  autoCompileOpts: {
    autoCompile: true,
    tsNodeOpts: {
      project: "./tsconfig.json",
      transpileOnly: true,
    },
  },
  specs: ["./test/specs/**/*.ts"],
  maxInstances: 10,
  capabilities: [
    {
      browserName: "vscode",
      browserVersion: VSCodeVersion,
      "wdio:vscodeOptions": {
        // points to directory where extension package.json is located
        extensionPath: join(__dirname, ExtensionPath),
        // used as e2e workspace
        workspacePath: TestsWorkspace,
        verboseLogging: true,
        vscodeArgs: {
          force: true,
          // installs an extension from the specified extension.vsix file
          // TODO wdio is ignoring this option, need to investigate
          installExtension: join(__dirname, ExtensionInstallerPath),
          disableExtensions: false,
        },
      } as VSCodeOptions,
    },
  ],
  outputDir: "wdio-logs",
  logLevel: "warn",
  bail: 0,
  waitforTimeout: 10000,
  connectionRetryTimeout: 120000,
  connectionRetryCount: 0,
  services: ["vscode"],
  framework: "mocha",
  reporters: ["spec"],
  mochaOpts: {
    ui: "bdd",
    timeout: 60000,
  },
};
