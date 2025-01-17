import { browser } from "@wdio/globals";
import * as h from "../helpers";
import * as v from "../validations";

describe("VS Code Extension Testing", () => {
  it("should be able to load VSCode and see Snowflake extension in Activity Bar", async () => {
    const workbench = await browser.getWorkbench();
    await v.validateIsVSCodeRunning(workbench);
    await v.validateIsSnowflakeExtensionInstalled(workbench);
    await h.openSnowflakeExtensionView(workbench);
  });

  it("should validate initial default settings", async () => {});

  it("should enable checkpoints feature", async () => {});
});
