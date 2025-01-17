import { browser, expect } from "@wdio/globals";
import { openSnowflakeExtensionView, wait } from "../utils";

describe("VS Code Extension Testing", () => {
  it("should be able to load VSCode", async () => {
    const workbench = await browser.getWorkbench();
    await openSnowflakeExtensionView(workbench);
    await wait(500);
    expect(await workbench.getTitleBar().getTitle()).toContain("[Extension Development Host]");
    const viewContainers = await workbench.getActivityBar().getViewControls();
    const titles = await Promise.all(viewContainers.map(async (vc) => await vc.getTitle()));
    expect(titles).toContain('Snowflake');
  });
});
