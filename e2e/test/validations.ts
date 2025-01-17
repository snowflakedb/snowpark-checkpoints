import { Workbench } from "wdio-vscode-service";
import { ExtensionKey } from "./constants";

export const validateIsVSCodeRunning = async (workbench: Workbench) => {
  const title = await workbench.getTitleBar().getTitle();
  expect(title).toContain("[Extension Development Host]");
};

export const validateIsSnowflakeExtensionInstalled = async (workbench: Workbench) => {
  const viewContainers = await workbench.getActivityBar().getViewControls();
  const titles = await Promise.all(viewContainers.map(async (vc) => await vc.getTitle()));
  expect(titles).toContain(ExtensionKey);
};
